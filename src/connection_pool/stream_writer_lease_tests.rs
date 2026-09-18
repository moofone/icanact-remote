use super::*;
use crate::AskResponder;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{Notify, Semaphore};

struct PendingWriter;

struct CaptureWriter {
    bytes: Vec<u8>,
}

impl CaptureWriter {
    fn new() -> Self {
        Self { bytes: Vec::new() }
    }
}

impl AsyncWrite for CaptureWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        this.bytes.extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

struct PendingOnceWriter {
    bytes: Vec<u8>,
    first_write_limit: usize,
    partial_write: bool,
    pending_write: bool,
    pending_flush: bool,
}

impl PendingOnceWriter {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            first_write_limit: 20,
            partial_write: true,
            pending_write: false,
            pending_flush: false,
        }
    }

    fn append_vectored(&mut self, bufs: &[std::io::IoSlice<'_>], limit: usize) -> usize {
        let mut remaining = limit;
        for buf in bufs {
            let take = remaining.min(buf.len());
            self.bytes.extend_from_slice(&buf[..take]);
            remaining -= take;
            if remaining == 0 {
                break;
            }
        }
        limit - remaining
    }
}

impl AsyncWrite for PendingOnceWriter {
    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.pending_write {
            return Poll::Pending;
        }
        let limit = if self.partial_write {
            self.partial_write = false;
            self.pending_write = true;
            self.first_write_limit.min(bytes.len())
        } else {
            bytes.len()
        };
        self.bytes.extend_from_slice(&bytes[..limit]);
        Poll::Ready(Ok(limit))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        if self.pending_write {
            return Poll::Pending;
        }
        let available = bufs.iter().map(|buf| buf.len()).sum::<usize>();
        let limit = if self.partial_write {
            self.partial_write = false;
            self.pending_write = true;
            self.first_write_limit.min(available)
        } else {
            available
        };
        let written = self.append_vectored(bufs, limit);
        Poll::Ready(Ok(written))
    }

    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if self.pending_flush {
            self.pending_flush = false;
            return Poll::Pending;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for PendingWriter {
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

struct PartialWriteWriter {
    bytes: Vec<u8>,
    first_write_limit: usize,
    writes: usize,
}

struct ParkAwareDuplex {
    inner: tokio::io::DuplexStream,
    parked: Arc<Notify>,
    saw_pending: Arc<std::sync::atomic::AtomicBool>,
}

impl AsyncRead for ParkAwareDuplex {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for ParkAwareDuplex {
    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let poll = Pin::new(&mut self.inner).poll_write(cx, bytes);
        if poll.is_pending() {
            self.saw_pending
                .store(true, std::sync::atomic::Ordering::Release);
            self.parked.notify_waiters();
        }
        poll
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let poll = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        if poll.is_pending() {
            self.saw_pending
                .store(true, std::sync::atomic::Ordering::Release);
            self.parked.notify_waiters();
        }
        poll
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl AsyncWrite for PartialWriteWriter {
    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let limit = if self.writes == 0 {
            self.first_write_limit.min(bytes.len())
        } else {
            bytes.len()
        };
        self.bytes.extend_from_slice(&bytes[..limit]);
        self.writes += 1;
        Poll::Ready(Ok(limit))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let available = bufs.iter().map(|buf| buf.len()).sum::<usize>();
        let limit = if self.writes == 0 {
            self.first_write_limit.min(available)
        } else {
            available
        };
        let mut remaining = limit;
        for buf in bufs {
            let take = remaining.min(buf.len());
            self.bytes.extend_from_slice(&buf[..take]);
            remaining -= take;
            if remaining == 0 {
                break;
            }
        }
        self.writes += 1;
        Poll::Ready(Ok(limit))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn suspended_partial_header_payload_and_flush_writes_resume() {
    let slots = crate::connection_pool::reply_slots::ReplySlots::new(
        1,
        "127.0.0.1:41016".parse().expect("test address"),
        Arc::new(Notify::new()),
    );
    let jobs = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(64));
    let record = slots
        .try_reserve(
            1,
            64,
            Arc::new(crate::ReplyPayload::from_static(b"cancelled")),
            jobs.try_acquire_owned().expect("job permit"),
            bytes.try_acquire_many_owned(64).expect("byte permit"),
        )
        .expect("lease reservation");
    record
        .publish(crate::ReplyPayload::from_static(b"streamed-payload"))
        .expect("normal publication");
    let mut pending = PendingStreamingCommand::local(StreamingCommand::LeasedResponse(
        Box::new(LeasedResponse {
            record,
            stage: LeasedResponseStage::Reserved,
        }),
    ));
    let mut writer = PendingOnceWriter::new();
    let next_stream_id = std::sync::atomic::AtomicU32::new(1);

    write_streaming_command_slice_with_context(
        &mut writer,
        &mut pending,
        1,
        1024,
        &next_stream_id,
    )
    .await
    .expect("select normal streaming stage");
    let (_, complete) = write_streaming_command_slice_with_context(
        &mut writer,
        &mut pending,
        1,
        1024,
        &next_stream_id,
    )
    .await
    .expect("commit a partial header and payload");
    assert!(!complete);
    assert_eq!(writer.bytes.len(), 20);
    {
        let future = write_streaming_command_slice_with_context(
            &mut writer,
            &mut pending,
            1,
            1024,
            &next_stream_id,
        );
        tokio::pin!(future);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
    }
    assert!(matches!(
        pending.command,
        StreamingCommand::LeasedResponse(ref response)
            if matches!(response.stage, LeasedResponseStage::WritingNormal(_))
    ));

    writer.pending_write = false;
    let (_, complete) = write_streaming_command_slice_with_context(
        &mut writer,
        &mut pending,
        1,
        1024,
        &next_stream_id,
    )
    .await
    .expect("resume payload after partial write");
    assert!(!complete);
    assert!(matches!(
        pending.command,
        StreamingCommand::LeasedResponse(ref response)
            if matches!(response.stage, LeasedResponseStage::Flushing)
    ));

    writer.pending_flush = true;
    {
        let future = write_streaming_command_slice_with_context(
            &mut writer,
            &mut pending,
            1,
            1024,
            &next_stream_id,
        );
        tokio::pin!(future);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
    }
    assert!(matches!(
        pending.command,
        StreamingCommand::LeasedResponse(ref response)
            if matches!(response.stage, LeasedResponseStage::Flushing)
    ));
    let (_, complete) = write_streaming_command_slice_with_context(
        &mut writer,
        &mut pending,
        1,
        1024,
        &next_stream_id,
    )
    .await
    .expect("resume flush after pending flush");
    assert!(complete);
    assert_eq!(slots.reserved(), 0);
    assert!(!writer.bytes.is_empty());
}

#[tokio::test]
async fn leased_write_keeps_frame_stage_when_write_future_is_dropped() {
    let slots = crate::connection_pool::reply_slots::ReplySlots::new(
        1,
        "127.0.0.1:41001".parse().unwrap(),
        Arc::new(Notify::new()),
    );
    let jobs = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(8));
    let record = slots
        .try_reserve(
            1,
            8,
            Arc::new(crate::ReplyPayload::from_static(b"cancelled")),
            jobs.try_acquire_owned().unwrap(),
            bytes.try_acquire_many_owned(8).unwrap(),
        )
        .unwrap();
    record.publish(crate::ReplyPayload::from_static(b"reply")).unwrap();
    let mut pending = PendingStreamingCommand::local(StreamingCommand::LeasedResponse(
        Box::new(LeasedResponse {
            record,
            stage: LeasedResponseStage::Reserved,
        }),
    ));
    let mut stream = PendingWriter;
    let next_stream_id = std::sync::atomic::AtomicU32::new(1);

    write_streaming_command_slice_with_context(
        &mut stream,
        &mut pending,
        1024,
        1024,
        &next_stream_id,
    )
    .await
    .unwrap();
    assert!(matches!(
        pending.command,
        StreamingCommand::LeasedResponse(ref response)
            if matches!(response.stage, LeasedResponseStage::WritingInline { .. })
    ));

    {
        let future = write_streaming_command_slice_with_context(
            &mut stream,
            &mut pending,
            1024,
            1024,
            &next_stream_id,
        );
        tokio::pin!(future);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
    }

    assert!(matches!(
        pending.command,
        StreamingCommand::LeasedResponse(ref response)
            if matches!(response.stage, LeasedResponseStage::WritingInline { .. })
    ));
}

#[test]
fn local_response_yield_retains_reservation_and_flush_order() {
    let mut queue = LocalStreamingQueue::new();
    let payload = bytes::Bytes::from_static(b"response-payload");
    queue
        .try_extend([
            StreamingCommand::BytesResponse(Box::new(BytesStreamingResponse::new(
                1,
                7,
                payload.clone(),
                payload.len(),
                STREAM_CHUNK_SIZE,
            ))),
            StreamingCommand::Flush,
        ])
        .expect("response bundle admission");

    let response = queue.pop_front().expect("response command");
    assert!(matches!(response, StreamingCommand::BytesResponse(_)));
    queue.response_yielded();
    assert_eq!(queue.retained_bytes(), payload.len());
    assert!(
        queue.pop_front().is_none(),
        "the paired Flush must remain behind a yielded response"
    );
    assert_eq!(
        queue.retained_bytes(),
        payload.len(),
        "the yielded response reservation must survive until its payload finishes"
    );

    // Model the response completing its final frame before the paired Flush
    // becomes eligible. The production writer performs this transition at the
    // same safe frame boundary.
    queue.response_finished();
    assert_eq!(queue.retained_bytes(), payload.len());
    assert!(matches!(queue.pop_front(), Some(StreamingCommand::Flush)));
    queue.flush_finished();
    assert_eq!(queue.retained_bytes(), 0);
}

#[test]
fn yielded_stream_commands_fit_the_combined_resumable_capacity() {
    let streaming_queue = StreamingQueue::new(
        crate::connection_pool::reply_slots::REPLY_SLOT_CAP,
        "127.0.0.1:41014".parse().expect("test address"),
    );
    let mut yielded = std::collections::VecDeque::new();
    let mut pending_slot = None;
    for _ in 0..(RESUMABLE_STREAM_COMMAND_CAP) {
        let mut pending = PendingStreamingCommand::local(StreamingCommand::WriteBytes(
            bytes::Bytes::from_static(b"bounded"),
        ));
        pending.yield_after_frame = true;
        finish_streaming_command_slice_owned(
            pending,
            false,
            &streaming_queue,
            &mut yielded,
            &mut pending_slot,
        );
    }
    assert_eq!(
        yielded.len(),
        RESUMABLE_STREAM_COMMAND_CAP,
        "resumed ownership must fit every admitted lease and local source"
    );
    assert!(
        pending_slot.is_none(),
        "a yielded command must never bypass source rotation through pending_stream_cmd"
    );
}

#[test]
fn resumed_queue_overflow_cannot_bypass_rotation() {
    let streaming_queue = StreamingQueue::new(
        crate::connection_pool::reply_slots::REPLY_SLOT_CAP,
        "127.0.0.1:41017".parse().expect("test address"),
    );
    let mut yielded = std::collections::VecDeque::new();
    let mut pending_slot = None;

    for _ in 0..crate::connection_pool::reply_slots::REPLY_SLOT_CAP {
        let mut pending = PendingStreamingCommand::local(StreamingCommand::WriteBytes(
            bytes::Bytes::from_static(b"bounded"),
        ));
        pending.yield_after_frame = true;
        finish_streaming_command_slice_owned(
            pending,
            false,
            &streaming_queue,
            &mut yielded,
            &mut pending_slot,
        );
    }

    let mut overflow = PendingStreamingCommand::local(StreamingCommand::WriteBytes(
        bytes::Bytes::from_static(b"overflow"),
    ));
    overflow.yield_after_frame = true;
    finish_streaming_command_slice_owned(
        overflow,
        false,
        &streaming_queue,
        &mut yielded,
        &mut pending_slot,
    );

    assert!(
        pending_slot.is_none(),
        "a yielded command must not enter pending_stream_cmd when the resumed set is full"
    );
    assert!(
        yielded.len() > crate::connection_pool::reply_slots::REPLY_SLOT_CAP,
        "the scheduler must retain overflow in its bounded resumed ownership"
    );
}

#[tokio::test]
async fn parked_ordinary_writer_wakes_only_after_lease_activation() {
    use tokio::io::AsyncReadExt;

    let addr = "127.0.0.1:41015".parse().expect("test address");
    let read_context = ReadContext {
        streaming_state_handoff: None,
        registry_weak: std::sync::Weak::new(),
        peer_addr: addr,
        session_source: addr,
        peer_id: None,
        max_message_size: MASTER_BUFFER_SIZE,
        expected_schema_hash: None,
        aligned_pool: Arc::new(crate::AlignedBytesPool::default()),
        inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
        response_correlation: None,
        response_writer: None,
        tell_handler_sync: None,
        tell_handler_sync_context: None,
        ask_immediate_handler_sync: None,
        ask_handler_sync: None,
        sync_actor_handler: None,
    };
    let (raw_io, mut peer) = tokio::io::duplex(64);
    let parked = Arc::new(Notify::new());
    let saw_pending = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let io = ParkAwareDuplex {
        inner: raw_io,
        parked: Arc::clone(&parked),
        saw_pending: Arc::clone(&saw_pending),
    };
    let (handle, writer_task, _reader_task) = LockFreeStreamHandle::new(
        io,
        addr,
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        Some(read_context),
    );
    let handle = Arc::new(handle);
    let ordinary = bytes::Bytes::from(vec![0x3Cu8; 128 * 1024]);
    let ordinary_header = crate::framing::try_write_ask_response_header(
        crate::MessageType::Response,
        41014,
        ordinary.len(),
    )
    .expect("ordinary frame header");
    handle
        .enqueue_write_nonblocking(WritePayload::HeaderInline {
            header: ordinary_header,
            header_len: 16,
            payload: ordinary.clone(),
        })
        .expect("ordinary write admission");
    // The tiny duplex is deliberately not drained yet. The transport wrapper
    // acknowledges the actual poll that parks the IO owner; no scheduler yield
    // or socket drain can satisfy this gate.
    let parked_wait = parked.notified();
    tokio::pin!(parked_wait);
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        &mut parked_wait,
    )
    .await
    .expect("the owner must reach the blocked ordinary write");
    assert!(saw_pending.load(std::sync::atomic::Ordering::Acquire));
    let budget = crate::ReplyDeliveryBudget::new(
        1,
        32,
        crate::ReplyPayload::from_static(b"duplicate-suppressed"),
    )
    .expect("valid budget");
    let lease = AskResponder::from_stream_handle(
        41015,
        Arc::clone(&handle),
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )
    .try_reply_lease(&budget, 32)
    .expect("lease admission");
    drop(lease);

    let terminal = b"duplicate-suppressed";
    let ordinary_wire_len = ordinary_header.len() + ordinary.len();
    let terminal_header = crate::framing::write_ask_response_header(
        crate::MessageType::Response,
        41015,
        terminal.len(),
    );
    let terminal_wire_len = terminal_header.len() + terminal.len();
    let ordinary_frame = {
        let mut frame = Vec::with_capacity(ordinary_wire_len);
        frame.extend_from_slice(&ordinary_header);
        frame.extend_from_slice(&ordinary);
        frame
    };
    let terminal_frame = {
        let mut frame = Vec::with_capacity(terminal_wire_len);
        frame.extend_from_slice(&terminal_header);
        frame.extend_from_slice(terminal);
        frame
    };
    let mut received_ordinary = vec![0u8; ordinary_wire_len];
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        peer.read_exact(&mut received_ordinary),
    )
    .await
    .expect("parked owner must finish the ordinary frame first")
    .expect("ordinary wire read");
    assert_eq!(received_ordinary, ordinary_frame);
    let mut received_terminal = vec![0u8; terminal_wire_len];
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        peer.read_exact(&mut received_terminal),
    )
    .await
    .expect("the lease wake must produce the terminal frame")
    .expect("terminal wire read");
    assert_eq!(received_terminal, terminal_frame);

    handle.shutdown();
    let _ = writer_task.await;
}

#[tokio::test]
async fn lease_notifier_idle_exit_race_does_not_lose_wakeup() {
    let exit_notify = Arc::new(Notify::new());
    let slots = crate::connection_pool::reply_slots::ReplySlots::new(
        1,
        "127.0.0.1:41012".parse().expect("test address"),
        Arc::clone(&exit_notify),
    );
    let jobs = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(16));
    let record = slots
        .try_reserve(
            1,
            16,
            Arc::new(crate::ReplyPayload::from_static(b"cancelled")),
            jobs.try_acquire_owned().expect("job permit"),
            bytes.try_acquire_many_owned(16).expect("byte permit"),
        )
        .expect("lease reservation");
    let lease_notify = slots.notify();
    // Register the exit waiter first: the pre-fix shared notifier delivers the
    // lease wake to this unrelated waiter, while the dedicated notifier keeps
    // it pending. This is the deterministic lost-wakeup reproduction.
    let exit_wait = exit_notify.notified();
    let lease_wait = lease_notify.notified();
    tokio::pin!(exit_wait);
    tokio::pin!(lease_wait);
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(exit_wait.as_mut().poll(&mut cx), Poll::Pending));
    assert!(matches!(lease_wait.as_mut().poll(&mut cx), Poll::Pending));

    record.activate();

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut lease_wait,)
            .await
            .is_ok()
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(1), &mut exit_wait,)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn saturated_sources_preserve_frame_boundary_rotation() {
    use tokio::io::AsyncReadExt;

    const PAYLOAD_LEN: usize = 4 * 1024 * 1024;
    const TERMINAL_CORRELATION: u32 = 10_001;
    let (io, mut peer) = tokio::io::duplex(64 * 1024 * 1024);
    let (handle, writer_task, _reader_task) = LockFreeStreamHandle::new(
        io,
        "127.0.0.1:41013".parse().expect("test address"),
        ChannelId::TellAsk,
        BufferConfig::new(256 * 1024).expect("test buffer"),
        None,
        None,
    );
    let handle = Arc::new(handle);
    let budget = crate::ReplyDeliveryBudget::new(
        crate::connection_pool::reply_slots::REPLY_SLOT_CAP,
        crate::connection_pool::reply_slots::REPLY_SLOT_CAP * PAYLOAD_LEN,
        crate::ReplyPayload::from_static(b"cancelled"),
    )
    .expect("valid budget");
    let long_payload = crate::ReplyPayload::copy_from_slice(&vec![0xA5; PAYLOAD_LEN]);
    let first = AskResponder::from_stream_handle(
        10_000,
        Arc::clone(&handle),
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )
    .try_reply_lease(&budget, PAYLOAD_LEN)
    .expect("long lease");
    first
        .try_reply_bytes(long_payload)
        .expect("long publication");
    let terminal = AskResponder::from_stream_handle(
        TERMINAL_CORRELATION,
        Arc::clone(&handle),
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )
    .try_reply_lease(&budget, PAYLOAD_LEN)
    .expect("terminal lease");
    drop(terminal);
    // Admit all remaining lease slots as short responses before the peer
    // starts draining. This is the continuous-admission side of the seam:
    // the IO owner must rotate each source rather than turning one yielded
    // command into a pinned pending response.
    for correlation_id in 10_002..10_064 {
        AskResponder::from_stream_handle(
            correlation_id,
            Arc::clone(&handle),
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )
        .try_reply_lease(&budget, PAYLOAD_LEN)
        .expect("occupied lease")
        .try_reply_bytes(crate::ReplyPayload::from_static(b"short"))
        .expect("short lease publication");
    }
    let ordinary_payload = bytes::Bytes::from_static(b"ordinary");
    let ordinary_header = crate::framing::try_write_ask_response_header(
        crate::MessageType::Response,
        30_000,
        ordinary_payload.len(),
    )
    .expect("ordinary frame header");
    handle
        .enqueue_write_nonblocking(WritePayload::HeaderInline {
            header: ordinary_header,
            header_len: 16,
            payload: ordinary_payload,
        })
        .expect("ordinary queue admission");
    let immediate_payload = bytes::Bytes::from_static(b"immediate");
    let immediate_header = crate::framing::try_write_ask_response_header(
        crate::MessageType::Response,
        30_001,
        immediate_payload.len(),
    )
    .expect("immediate frame header");
    handle
        .enqueue_immediate_write_nonblocking(WritePayload::HeaderInline {
            header: immediate_header,
            header_len: 16,
            payload: immediate_payload,
        })
        .expect("immediate queue admission");
    for correlation_id in 20_000..20_004 {
        handle
            .stream_response_bytes(bytes::Bytes::from_static(b"shared"), correlation_id)
            .await
            .expect("shared streaming traffic");
    }

    let mut stream_frames_before_terminal = 0usize;
    loop {
        let mut control = [0u8; crate::framing::LENGTH_PREFIX_LEN];
        peer.read_exact(&mut control).await.expect("wire control");
        let decoded = crate::framing::decode_control(control).expect("wire control encoding");
        let mut body = vec![0u8; decoded.body_len];
        peer.read_exact(&mut body).await.expect("wire frame body");
        if matches!(
            decoded.kind,
            crate::framing::WireKind::StreamResponseStart
                | crate::framing::WireKind::StreamResponseData
        ) {
            stream_frames_before_terminal += 1;
        }
        if decoded.kind == crate::framing::WireKind::Response
            && body.len() >= 4
            && u32::from_be_bytes(body[..4].try_into().expect("correlation bytes"))
                == TERMINAL_CORRELATION
        {
            break;
        }
    }
    assert!(
        stream_frames_before_terminal <= 4,
        "terminal lease was not bounded by frame rotation: {stream_frames_before_terminal} frames"
    );

    // Replenish the exact slot freed by the terminal lease while the other
    // sources remain active. Retry only the real admission race; the responder
    // stays unclaimed until capacity is actually returned.
    let replacement_correlation = 31_000;
    let mut replacement_responder = Some(AskResponder::from_stream_handle(
        replacement_correlation,
        Arc::clone(&handle),
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    ));
    let replacement = loop {
        let responder = replacement_responder
            .take()
            .expect("capacity retry retains the responder");
        match responder.try_reply_lease(&budget, PAYLOAD_LEN) {
            Ok(lease) => break lease,
            Err(error) => {
                replacement_responder = Some(
                    error
                        .into_responder()
                        .expect("capacity retry must preserve responder ownership"),
                );
                tokio::task::yield_now().await;
            }
        }
    };
    replacement
        .try_reply_bytes(crate::ReplyPayload::from_static(b"replenished"))
        .expect("replacement lease publication");
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let mut control = [0u8; crate::framing::LENGTH_PREFIX_LEN];
            peer.read_exact(&mut control).await.expect("replacement control");
            let decoded = crate::framing::decode_control(control).expect("replacement encoding");
            let mut body = vec![0u8; decoded.body_len];
            peer.read_exact(&mut body).await.expect("replacement body");
            if decoded.kind == crate::framing::WireKind::Response
                && body.len() >= 4
                && u32::from_be_bytes(body[..4].try_into().expect("replacement correlation"))
                    == replacement_correlation
            {
                break;
            }
        }
    })
    .await
    .expect("replacement admission must be serviced");

    handle.shutdown();
    let _ = writer_task.await;
}

#[tokio::test]
async fn rejected_sibling_claim_does_not_activate_a_terminal_reply() {
    use tokio::io::AsyncReadExt;

    let budget = crate::ReplyDeliveryBudget::new(
        2,
        64,
        crate::ReplyPayload::from_static(b"duplicate-suppressed"),
    )
    .unwrap();
    let (io, mut peer) = tokio::io::duplex(4096);
    let (handle, writer_task, _reader_task) = LockFreeStreamHandle::new(
        io,
        "127.0.0.1:41003".parse().unwrap(),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    let handle = Arc::new(handle);
    let used = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let first = crate::AskResponder::from_stream_handle(80, Arc::clone(&handle), Arc::clone(&used))
        .try_reply_lease(&budget, 32)
        .unwrap();
    let sibling = crate::AskResponder::from_stream_handle(80, Arc::clone(&handle), used);
    let rejected = sibling.try_reply_lease(&budget, 32).unwrap_err();
    assert!(matches!(
        rejected,
        crate::ReplyLeaseAdmissionError::ClaimUnavailable(_)
    ));

    let mut bytes = [0u8; crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN];
    let read = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        peer.read_exact(&mut bytes),
    )
    .await;
    assert!(read.is_err(), "rejected claim must not activate a wire reply");

    drop(first);
    handle.shutdown();
    let _ = writer_task.await;
}

#[test]
fn generation_exhaustion_does_not_consume_the_last_free_slot() {
    let slots = crate::connection_pool::reply_slots::ReplySlots::new(
        1,
        "127.0.0.1:41006".parse().unwrap(),
        Arc::new(Notify::new()),
    );
    slots.force_generation_exhaustion(0);
    let jobs = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(8));
    let result = slots.try_reserve(
        1,
        8,
        Arc::new(crate::ReplyPayload::from_static(b"cancelled")),
        jobs.try_acquire_owned().unwrap(),
        bytes.try_acquire_many_owned(8).unwrap(),
    );
    assert!(matches!(
        result,
        Err(crate::GossipError::Network(error))
            if error.kind() == std::io::ErrorKind::WouldBlock
    ));
    assert_eq!(slots.free_count(), crate::connection_pool::reply_slots::REPLY_SLOT_CAP);
}

#[tokio::test]
async fn stream_id_exhaustion_is_terminal_without_wraparound() {
    let (io, _peer) = tokio::io::duplex(4096);
    let (handle, writer_task, _reader_task) = LockFreeStreamHandle::new(
        io,
        "127.0.0.1:41007".parse().unwrap(),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    handle
        .next_stream_id
        .store(u32::MAX, std::sync::atomic::Ordering::Release);
    let jobs = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(16));
    let record = handle
        .reply_slots()
        .try_reserve(
            91,
            16,
            Arc::new(crate::ReplyPayload::from_static(b"cancelled")),
            jobs.try_acquire_owned().expect("job permit"),
            bytes.try_acquire_many_owned(16).expect("byte permit"),
        )
        .expect("lease reservation");
    record
        .publish(crate::ReplyPayload::from_static(b"normal"))
        .expect("normal publication");
    let mut response = LeasedResponse {
        record,
        stage: LeasedResponseStage::Reserved,
    };
    let (mut writer, _peer) = tokio::io::duplex(4096);
    let mut pending_offset = 0;
    let next_stream_id = std::sync::atomic::AtomicU32::new(u32::MAX);
    write_leased_response_command_slice(
        &mut writer,
        &mut pending_offset,
        &mut response,
        1,
        1024,
        &next_stream_id,
    )
    .await
    .expect("terminal fallback write setup");
    assert!(matches!(
        response.stage,
        LeasedResponseStage::WritingTerminal { .. }
    ));
    assert!(
        !handle
            .shutdown_signal
            .load(std::sync::atomic::Ordering::Acquire),
        "lease stream-id exhaustion must use terminal fallback, not connection shutdown"
    );
    handle.shutdown();
    let _ = writer_task.await;
}

#[tokio::test]
async fn production_abort_terminal_settles_correlation() {
    use tokio::io::AsyncWriteExt;

    let addr = "127.0.0.1:41019".parse().expect("test address");
    let context = ReadContext {
        streaming_state_handoff: None,
        registry_weak: std::sync::Weak::new(),
        peer_addr: addr,
        session_source: addr,
        peer_id: None,
        max_message_size: MASTER_BUFFER_SIZE,
        expected_schema_hash: None,
        aligned_pool: Arc::new(crate::AlignedBytesPool::default()),
        inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
        response_correlation: None,
        response_writer: None,
        tell_handler_sync: None,
        tell_handler_sync_context: None,
        ask_immediate_handler_sync: None,
        ask_handler_sync: None,
        sync_actor_handler: None,
    };
    let tracker = Arc::new(CorrelationTracker::new());
    let guard = tracker.allocate().expect("correlation slot");
    let correlation_id = guard.id();
    let registry = Arc::new(crate::registry::GossipRegistry::new(
        addr,
        crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing("lease-parser-test")),
            ..crate::GossipConfig::default()
        },
    ));
    let (mut source, mut parser) = tokio::io::duplex(4096);
    let mut read_state = ReadState::new();
    let mut streaming_state = crate::protocol::StreamingState::new();

    crate::protocol::process_read_result(
        crate::handle::MessageReadResult::Streaming {
            msg_type: crate::MessageType::StreamResponseStart as u8,
            correlation_id,
            schema_hash: None,
            stream_header: crate::StreamHeader {
                stream_id: 7,
                total_size: 8,
                chunk_size: 4,
                chunk_index: 0,
                type_hash: 0,
                actor_id: 0,
            },
            chunk_data: bytes::Bytes::from_static(b"part"),
        },
        &mut streaming_state,
        &registry,
        addr,
        addr,
        Some(&tracker),
        None,
        None,
    )
    .await
    .expect("production stream-start processing");
    assert_eq!(streaming_state.active_stream_count(), 1);

    let abort = crate::framing::write_stream_abort_header(7, 9);
    source.write_all(&abort).await.expect("abort frame write");
    let parsed_abort = {
        let mut result = None;
        for _ in 0..16 {
            if let Some(parsed) = read_message_step(
                &mut parser,
                &mut read_state,
                &context,
                &mut streaming_state,
            )
            .await
            .expect("parse abort frame")
            {
                result = Some(parsed);
                break;
            }
        }
        result.expect("abort result")
    };
    assert!(matches!(
        parsed_abort,
        crate::handle::MessageReadResult::StreamAbort {
            stream_id: 7,
            reason: 9
        }
    ));
    crate::protocol::process_read_result(
        parsed_abort,
        &mut streaming_state,
        &registry,
        addr,
        addr,
        Some(&tracker),
        None,
        None,
    )
    .await
    .expect("production stream-abort processing");
    assert_eq!(streaming_state.active_stream_count(), 0);

    let tracker_for_wait = Arc::clone(&tracker);
    let waiter = tokio::spawn(async move {
        tracker_for_wait
            .wait_for_response_no_timeout(correlation_id)
            .await
    });
    let terminal = b"cancelled";
    let header = crate::framing::write_ask_response_header(
        crate::MessageType::Response,
        correlation_id,
        terminal.len(),
    );
    source.write_all(&header).await.expect("terminal header write");
    source.write_all(terminal).await.expect("terminal payload write");
    let parsed_response = {
        let mut result = None;
        for _ in 0..16 {
            if let Some(parsed) = read_message_step(
                &mut parser,
                &mut read_state,
                &context,
                &mut streaming_state,
            )
            .await
            .expect("parse terminal response")
            {
                result = Some(parsed);
                break;
            }
        }
        result.expect("terminal response result")
    };
    let crate::handle::MessageReadResult::Response {
        correlation_id: parsed_id,
        payload,
    } = parsed_response
    else {
        panic!("terminal frame must parse as a response");
    };
    assert_eq!(parsed_id, correlation_id);
    crate::protocol::process_read_result(
        crate::handle::MessageReadResult::Response {
            correlation_id: parsed_id,
            payload,
        },
        &mut streaming_state,
        &registry,
        addr,
        addr,
        Some(&tracker),
        None,
        None,
    )
    .await
    .expect("production terminal-response processing");
    let received = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        waiter,
    )
    .await
    .expect("untimed requester must settle")
    .expect("requester task must not panic")
    .expect("terminal response must settle correlation");
    assert_eq!(received.as_ref(), b"cancelled");
    guard.disarm();
}

#[tokio::test]
async fn final_frame_cancellation_is_too_late_after_real_io_commit() {
    use tokio::io::AsyncReadExt;

    let slots = crate::connection_pool::reply_slots::ReplySlots::new(
        1,
        "127.0.0.1:41018".parse().expect("test address"),
        Arc::new(Notify::new()),
    );
    let jobs = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(64));
    let record = slots
        .try_reserve(
            18,
            64,
            Arc::new(crate::ReplyPayload::from_static(b"cancelled")),
            jobs.try_acquire_owned().expect("job permit"),
            bytes.try_acquire_many_owned(64).expect("byte permit"),
        )
        .expect("lease reservation");
    record
        .publish(crate::ReplyPayload::from_static(b"final-frame"))
        .expect("normal publication");
    let normal_len = crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN + b"final-frame".len();
    let mut response = LeasedResponse {
        record: Arc::clone(&record),
        stage: LeasedResponseStage::Reserved,
    };
    let (mut writer, mut peer) = tokio::io::duplex(4096);
    let next_stream_id = std::sync::atomic::AtomicU32::new(1);
    let mut pending_offset = 0;

    write_leased_response_command_slice(
        &mut writer,
        &mut pending_offset,
        &mut response,
        usize::MAX,
        1024,
        &next_stream_id,
    )
    .await
    .expect("select final inline frame");
    let (_, complete, _) = write_leased_response_command_slice(
        &mut writer,
        &mut pending_offset,
        &mut response,
        usize::MAX,
        1024,
        &next_stream_id,
    )
    .await
    .expect("commit final inline frame");
    assert!(!complete);
    assert!(matches!(response.stage, LeasedResponseStage::Flushing));

    // The final frame is already selected and committed; cancellation must be
    // observed as too late even though the lease remains reserved until flush.
    record.cancel();
    let (_, complete, _) = write_leased_response_command_slice(
        &mut writer,
        &mut pending_offset,
        &mut response,
        usize::MAX,
        1024,
        &next_stream_id,
    )
    .await
    .expect("flush committed frame");
    assert!(complete);

    let mut wire = vec![0u8; normal_len];
    peer.read_exact(&mut wire).await.expect("real IO frame");
    assert_eq!(wire.len(), normal_len);
    drop(response);
    drop(record);
    while slots.pop_ready().is_some() {}
    let stats = slots.stats_snapshot();
    assert_eq!(stats.normal_completions, 1);
    assert_eq!(stats.too_late_cancellations, 1);
    assert_eq!(stats.reserved_jobs, 0);
    assert_eq!(stats.reserved_bytes, 0);
}

#[tokio::test]
async fn partial_final_frame_cancel_counts_too_late_once() {
    let slots = crate::connection_pool::reply_slots::ReplySlots::new(
        1,
        "127.0.0.1:41020".parse().expect("test address"),
        Arc::new(Notify::new()),
    );
    let jobs = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(64));
    let record = slots
        .try_reserve(
            20,
            64,
            Arc::new(crate::ReplyPayload::from_static(b"cancelled")),
            jobs.try_acquire_owned().expect("job permit"),
            bytes.try_acquire_many_owned(64).expect("byte permit"),
        )
        .expect("lease reservation");
    record
        .publish(crate::ReplyPayload::from_static(b"committed-final-frame"))
        .expect("normal publication");
    let mut response = LeasedResponse {
        record: Arc::clone(&record),
        stage: LeasedResponseStage::Reserved,
    };
    let mut writer = PartialWriteWriter {
        bytes: Vec::new(),
        first_write_limit: 1,
        writes: 0,
    };
    let next_stream_id = std::sync::atomic::AtomicU32::new(1);
    let mut pending_offset = 0;

    write_leased_response_command_slice(
        &mut writer,
        &mut pending_offset,
        &mut response,
        usize::MAX,
        1024,
        &next_stream_id,
    )
    .await
    .expect("select inline final frame");
    let (written, complete, _) = write_leased_response_command_slice(
        &mut writer,
        &mut pending_offset,
        &mut response,
        usize::MAX,
        1024,
        &next_stream_id,
    )
    .await
    .expect("commit only the first final-frame byte");
    assert_eq!(written, 1);
    assert!(!complete);
    assert!(matches!(response.stage, LeasedResponseStage::WritingInline { .. }));

    record.cancel();
    let (_, next_complete, _) = write_leased_response_command_slice(
        &mut writer,
        &mut pending_offset,
        &mut response,
        usize::MAX,
        1024,
        &next_stream_id,
    )
    .await
    .expect("resume committed final frame after cancellation");
    assert!(!next_complete);
    assert!(matches!(response.stage, LeasedResponseStage::Flushing));
    let (_, complete, _) = write_leased_response_command_slice(
        &mut writer,
        &mut pending_offset,
        &mut response,
        usize::MAX,
        1024,
        &next_stream_id,
    )
    .await
    .expect("flush committed final frame");
    assert!(complete);
    drop(response);
    drop(record);
    assert_eq!(slots.reserved(), 0);
    let stats = slots.stats_snapshot();
    assert_eq!(stats.normal_completions, 1);
    assert_eq!(stats.too_late_cancellations, 1);
    assert_eq!(stats.cancellation_publications, 1);
}

#[tokio::test]
async fn cancellation_between_frames_aborts_then_settles_terminal_reply_lease_stats() {
    let slots = crate::connection_pool::reply_slots::ReplySlots::new(
        1,
        "127.0.0.1:41008".parse().unwrap(),
        Arc::new(Notify::new()),
    );
    let jobs = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(16));
    let record = slots
        .try_reserve(
            90,
            16,
            Arc::new(crate::ReplyPayload::from_static(b"cancelled")),
            jobs.try_acquire_owned().unwrap(),
            bytes.try_acquire_many_owned(16).unwrap(),
        )
        .unwrap();
    let normal = BytesStreamingResponse::new(
        7,
        90,
        bytes::Bytes::from_static(b"abcdefghijklmnopqrstuvwxyzabcdef"),
        32,
        8,
    );
    let mut response = LeasedResponse {
        record: Arc::clone(&record),
        stage: LeasedResponseStage::WritingNormal(Box::new(normal)),
    };
    let mut writer = CaptureWriter::new();
    let mut pending_offset = 0;
    let next_stream_id = std::sync::atomic::AtomicU32::new(1);
    let mut frame_boundary = false;
    let mut complete = false;
    while !frame_boundary && !complete {
        let (_, next_complete, next_boundary) = write_leased_response_command_slice(
            &mut writer,
            &mut pending_offset,
            &mut response,
            1,
            1024,
            &next_stream_id,
        )
        .await
        .unwrap();
        complete = next_complete;
        frame_boundary = next_boundary;
    }
    assert!(!complete);
    assert!(
        frame_boundary,
        "the first real frame must commit before cancellation"
    );
    assert!(matches!(
        response.stage,
        LeasedResponseStage::WritingNormal(_)
    ));
    let first_control: [u8; crate::framing::LENGTH_PREFIX_LEN] = writer.bytes
        [..crate::framing::LENGTH_PREFIX_LEN]
        .try_into()
        .expect("first control bytes");
    let first = crate::framing::decode_control(first_control).expect("first control");
    assert_eq!(first.kind, crate::framing::WireKind::StreamResponseStart);
    let first_end = crate::framing::LENGTH_PREFIX_LEN + first.body_len;
    assert_eq!(writer.bytes.len(), first_end);
    record.cancel();
    let (_, complete, _) = write_leased_response_command_slice(
        &mut writer,
        &mut pending_offset,
        &mut response,
        1,
        1024,
        &next_stream_id,
    )
    .await
    .unwrap();
    assert!(!complete);
    assert!(matches!(
        response.stage,
        LeasedResponseStage::WritingAbort { .. }
    ));
    let (_, complete, _) = write_leased_response_command_slice(
        &mut writer,
        &mut pending_offset,
        &mut response,
        1,
        1024,
        &next_stream_id,
    )
    .await
    .unwrap();
    assert!(!complete);
    assert!(matches!(
        response.stage,
        LeasedResponseStage::WritingTerminal { .. }
    ));
    let abort_start = first_end;
    let abort: [u8; crate::framing::STREAM_DATA_FRAME_HEADER_LEN] = writer.bytes
        [abort_start..abort_start + crate::framing::STREAM_DATA_FRAME_HEADER_LEN]
        .try_into()
        .expect("abort frame");
    assert_eq!(
        crate::framing::decode_control(abort[..4].try_into().expect("abort control"))
            .expect("abort control")
            .kind,
        crate::framing::WireKind::StreamAbort
    );
    let (_, complete, _) = write_leased_response_command_slice(
        &mut writer,
        &mut pending_offset,
        &mut response,
        1,
        1024,
        &next_stream_id,
    )
    .await
    .unwrap();
    assert!(!complete);
    assert!(matches!(
        response.stage,
        LeasedResponseStage::WritingTerminal { .. }
    ));
    let (_, complete, _) = write_leased_response_command_slice(
        &mut writer,
        &mut pending_offset,
        &mut response,
        1,
        1024,
        &next_stream_id,
    )
    .await
    .unwrap();
    assert!(!complete);
    assert!(matches!(response.stage, LeasedResponseStage::Flushing));
    let terminal_start = abort_start + crate::framing::STREAM_DATA_FRAME_HEADER_LEN;
    let terminal_header: [u8; crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN] = writer.bytes
        [terminal_start..terminal_start + crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN]
        .try_into()
        .expect("terminal header");
    assert_eq!(
        crate::framing::decode_control(terminal_header[..4].try_into().expect("response control"))
            .expect("response control")
            .kind,
        crate::framing::WireKind::Response
    );
    let terminal_payload_start = terminal_start + crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN;
    assert_eq!(
        &writer.bytes[terminal_payload_start..terminal_payload_start + 9],
        b"cancelled"
    );
    let (_, complete, _) = write_leased_response_command_slice(
        &mut writer,
        &mut pending_offset,
        &mut response,
        1,
        1024,
        &next_stream_id,
    )
    .await
    .unwrap();
    assert!(complete);
    assert_eq!(slots.reserved(), 0);
    let stats = slots.stats_snapshot();
    assert_eq!(
        stats.actual_stream_aborts, 1,
        "an abort frame is counted only when cancellation is observed between frames"
    );
    assert!(stats.terminal_completions >= 1);
}

#[tokio::test]
async fn io_exit_reclaims_ready_active_and_pending_ownership() {
    let (io, _peer) = tokio::io::duplex(64 * 1024);
    let (handle, writer_task, _reader_task) = LockFreeStreamHandle::new(
        io,
        "127.0.0.1:41021".parse().expect("test address"),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    let handle = Arc::new(handle);
    let budget = crate::ReplyDeliveryBudget::new(
        3,
        3 * 64,
        crate::ReplyPayload::from_static(b"cancelled"),
    )
    .expect("valid budget");
    let pending = AskResponder::from_stream_handle(
        21,
        Arc::clone(&handle),
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )
    .try_reply_lease(&budget, 64)
    .expect("pending lease admission");
    let ready = AskResponder::from_stream_handle(
        22,
        Arc::clone(&handle),
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )
    .try_reply_lease(&budget, 64)
    .expect("ready lease admission");
    drop(ready);
    let active = AskResponder::from_stream_handle(
        23,
        Arc::clone(&handle),
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )
    .try_reply_lease(&budget, 64)
    .expect("active lease admission");
    active
        .try_reply_bytes(crate::ReplyPayload::from_static(b"active"))
        .expect("active publication");

    handle.shutdown();
    tokio::time::timeout(std::time::Duration::from_secs(2), writer_task)
        .await
        .expect("IO exit must reclaim the writer task")
        .expect("writer task must not panic");
    drop(pending);
    let stats = handle.reply_slots().stats_snapshot();
    assert_eq!(stats.live_slots, 0);
    assert_eq!(stats.reserved_jobs, 0);
    assert_eq!(stats.reserved_bytes, 0);
    assert_eq!(handle.reply_slots().reserved(), 0);
    assert_eq!(stats.normal_completions, 0);
    assert_eq!(stats.discarded_reservations, 0);
}

#[tokio::test]
async fn closing_slots_reclaims_and_reports_connection_closed() {
    let slots = crate::connection_pool::reply_slots::ReplySlots::new(
        1,
        "127.0.0.1:41005".parse().unwrap(),
        Arc::new(Notify::new()),
    );
    let jobs = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(32));
    let record = slots
        .try_reserve(
            1,
            9,
            Arc::new(crate::ReplyPayload::from_static(b"cancelled")),
            jobs.try_acquire_owned().unwrap(),
            bytes.try_acquire_many_owned(9).unwrap(),
        )
        .unwrap();
    let waiting = record.wait_complete();
    tokio::pin!(waiting);
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(waiting.as_mut().poll(&mut cx), Poll::Pending));

    slots.close_and_reclaim();
    record.activate();
    let result = waiting.await;
    assert!(matches!(
        result,
        Err(crate::GossipError::ConnectionClosed(addr))
            if addr == "127.0.0.1:41005".parse::<std::net::SocketAddr>().unwrap()
    ));
    assert_eq!(slots.reserved(), 0);
}

#[tokio::test]
async fn dropping_unpolled_reply_future_cancels_reserved_record() {
    let slots = crate::connection_pool::reply_slots::ReplySlots::new(
        1,
        "127.0.0.1:41009".parse().unwrap(),
        Arc::new(Notify::new()),
    );
    let jobs = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(16));
    let record = slots
        .try_reserve(
            1,
            16,
            Arc::new(crate::ReplyPayload::from_static(b"cancelled")),
            jobs.try_acquire_owned().unwrap(),
            bytes.try_acquire_many_owned(16).unwrap(),
        )
        .unwrap();
    let record_for_assertion = Arc::clone(&record);
    let (io, _peer) = tokio::io::duplex(64);
    let (handle, writer_task, _reader_task) = LockFreeStreamHandle::new(
        io,
        "127.0.0.1:41010".parse().unwrap(),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    let handle = Arc::new(handle);
    let lease = crate::ReplyLease::new(Arc::clone(&handle), record);
    let future = lease.reply_bytes(crate::ReplyPayload::from_static(b"reply"));
    drop(future);
    assert!(record_for_assertion.is_cancelled());
    handle.shutdown();
    let _ = writer_task.await;
}

#[tokio::test]
async fn flush_pending_keeps_leased_stage_for_resume() {
    let slots = crate::connection_pool::reply_slots::ReplySlots::new(
        1,
        "127.0.0.1:41011".parse().unwrap(),
        Arc::new(Notify::new()),
    );
    let jobs = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(8));
    let record = slots
        .try_reserve(
            1,
            8,
            Arc::new(crate::ReplyPayload::from_static(b"cancelled")),
            jobs.try_acquire_owned().unwrap(),
            bytes.try_acquire_many_owned(8).unwrap(),
        )
        .unwrap();
    let mut response = LeasedResponse {
        record,
        stage: LeasedResponseStage::Flushing,
    };
    let mut stream = PendingWriter;
    let next_stream_id = std::sync::atomic::AtomicU32::new(1);
    let mut pending_offset = 0;
    {
        let future = write_leased_response_command_slice(
            &mut stream,
            &mut pending_offset,
            &mut response,
            1,
            1024,
            &next_stream_id,
        );
        tokio::pin!(future);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
    }
    assert!(matches!(response.stage, LeasedResponseStage::Flushing));
}

#[tokio::test]
async fn lease_preserves_reply_observer_ownership_once() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Observer(AtomicUsize);
    impl crate::AskReplyObserver for Observer {
        fn reply_claimed(&self, _payload: bytes::Bytes) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let (io, _peer) = tokio::io::duplex(4096);
    let (handle, writer_task, _reader_task) = LockFreeStreamHandle::new(
        io,
        "127.0.0.1:41004".parse().unwrap(),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    let handle = Arc::new(handle);
    let observer = Arc::new(Observer(AtomicUsize::new(0)));
    let context = crate::AskContext::from_stream_handle_with_request_id(81, &handle, None, None)
        .with_reply_observer(Arc::clone(&observer) as Arc<dyn crate::AskReplyObserver>);
    let budget =
        crate::ReplyDeliveryBudget::new(1, 32, crate::ReplyPayload::from_static(b"cancelled"))
            .unwrap();
    let lease = context.responder().try_reply_lease(&budget, 9).unwrap();
    lease
        .try_reply_bytes(crate::ReplyPayload::from_static(b"reply"))
        .unwrap();
    assert_eq!(observer.0.load(Ordering::SeqCst), 1);

    handle.shutdown();
    let _ = writer_task.await;
}
