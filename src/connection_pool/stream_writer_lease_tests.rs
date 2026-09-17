use super::*;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::sync::Arc;
use tokio::io::AsyncWrite;
use tokio::sync::{Notify, Semaphore};

struct PendingWriter;

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
fn scheduler_keeps_each_yielded_lease_progress_record() {
    let queue = StreamingQueue::new(8, "127.0.0.1:41002".parse().unwrap());
    let mut yielded = std::collections::VecDeque::new();
    let mut pending = None;
    let first = PendingStreamingCommand {
        command: StreamingCommand::WriteBytes(bytes::Bytes::from_static(b"first")),
        offset: 1,
        from_shared_queue: false,
        yield_after_frame: true,
    };
    let second = PendingStreamingCommand {
        command: StreamingCommand::WriteBytes(bytes::Bytes::from_static(b"second")),
        offset: 2,
        from_shared_queue: false,
        yield_after_frame: true,
    };
    finish_streaming_command_slice_owned(first, false, &queue, &mut yielded, &mut pending);
    finish_streaming_command_slice_owned(second, false, &queue, &mut yielded, &mut pending);
    for offset in 3..=64 {
        finish_streaming_command_slice_owned(
            PendingStreamingCommand {
                command: StreamingCommand::WriteBytes(bytes::Bytes::from_static(b"more")),
                offset,
                from_shared_queue: false,
                yield_after_frame: true,
            },
            false,
            &queue,
            &mut yielded,
            &mut pending,
        );
    }
    assert_eq!(yielded.len(), 64);
    assert_eq!(yielded.front().map(|value| value.offset), Some(1));
    assert_eq!(yielded.back().map(|value| value.offset), Some(64));
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
    let (io, _peer) = tokio::io::duplex(64);
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
    assert!(matches!(handle.allocate_stream_id(), Err(crate::GossipError::Shutdown)));
    assert!(handle.shutdown_signal.load(std::sync::atomic::Ordering::Acquire));
    handle.shutdown();
    let _ = writer_task.await;
}

#[tokio::test]
async fn cancellation_between_frames_aborts_then_settles_terminal_reply() {
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
    record.cancel();
    let mut normal = BytesStreamingResponse::new(
        7,
        90,
        bytes::Bytes::from_static(b"abcdefghijklmnop"),
        16,
        8,
    );
    normal.frame_offset = 1;
    let mut response = LeasedResponse {
        record,
        stage: LeasedResponseStage::WritingNormal(Box::new(normal)),
    };
    let (mut writer, _peer) = tokio::io::duplex(4096);
    let mut pending_offset = 1;
    let next_stream_id = std::sync::atomic::AtomicU32::new(1);
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
    assert!(matches!(response.stage, LeasedResponseStage::WritingAbort { .. }));

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
    let context = crate::AskContext::from_stream_handle_with_request_id(
        81,
        &handle,
        None,
        None,
    )
    .with_reply_observer(Arc::clone(&observer) as Arc<dyn crate::AskReplyObserver>);
    let budget = crate::ReplyDeliveryBudget::new(
        1,
        32,
        crate::ReplyPayload::from_static(b"cancelled"),
    )
    .unwrap();
    let lease = context
        .responder()
        .try_reply_lease(&budget, 9)
        .unwrap();
    lease
        .try_reply_bytes(crate::ReplyPayload::from_static(b"reply"))
        .unwrap();
    assert_eq!(observer.0.load(Ordering::SeqCst), 1);

    handle.shutdown();
    let _ = writer_task.await;
}
