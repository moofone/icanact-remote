use std::sync::{Arc, atomic::AtomicBool};
use tokio::sync::Semaphore;

use bytes::Bytes;

use crate::connection_pool::{BufferConfig, ChannelId, LockFreeStreamHandle};
use crate::{AskReplyObserver, AskResponder, ReplyDeliveryBudget, ReplyPayload};

#[test]
fn budget_rejects_cancellation_payload_larger_than_byte_limit() {
    let result = ReplyDeliveryBudget::new(1, 3, ReplyPayload::from_static(b"cancelled"));
    assert!(
        result.is_err(),
        "an admitted budget must carry its cancellation payload"
    );
}

#[tokio::test]
async fn slot_exhaustion_does_not_claim_responder() {
    let budget =
        ReplyDeliveryBudget::new(64, 64 * b"reply".len(), ReplyPayload::from_static(b"dupe"))
            .expect("valid reply budget");
    let (io, _peer) = tokio::io::duplex(4096);
    let (handle, _writer_task, _reader_task) = LockFreeStreamHandle::new(
        io,
        "127.0.0.1:40564".parse().expect("test address"),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    let handle = Arc::new(handle);
    let reply_session =
        crate::AskContext::from_stream_handle_with_request_id(0, &handle, None, None)
            .reply_session()
            .unwrap();
    let delivery = budget.bind(&reply_session).unwrap();

    let mut leases = Vec::with_capacity(64);
    for correlation_id in 0..64 {
        let responder = AskResponder::from_stream_handle(
            correlation_id,
            Arc::clone(&handle),
            Arc::new(AtomicBool::new(false)),
        );
        leases.push(
            responder
                .try_reply_lease(&delivery, Bytes::from_static(b"reply").len())
                .expect("each reserved slot admits one responder"),
        );
    }

    let responder =
        AskResponder::from_stream_handle(64, Arc::clone(&handle), Arc::new(AtomicBool::new(false)));
    let error = responder
        .try_reply_lease(&delivery, 5)
        .expect_err("the bounded connection slot set is exhausted");
    let responder = error
        .into_responder()
        .expect("admission failure preserves responder");
    assert!(
        !responder
            .try_reply_bytes(crate::ForwardPayload::from_static(b"still-owned"))
            .is_err_and(|error| matches!(
                error,
                crate::GossipError::Network(ref error)
                    if error.kind() == std::io::ErrorKind::AlreadyExists
            )),
        "failed slot admission must not claim the responder"
    );

    drop(leases);
    handle.shutdown();
}

#[tokio::test]
async fn cancel_before_start_writes_only_duplicate_suppressed() {
    use tokio::io::AsyncReadExt;

    let budget =
        ReplyDeliveryBudget::new(1, 64, ReplyPayload::from_static(b"duplicate-suppressed"))
            .expect("valid reply budget");
    let (io, mut peer) = tokio::io::duplex(4096);
    let (handle, writer_task, _reader_task) = LockFreeStreamHandle::new(
        io,
        "127.0.0.1:40565".parse().expect("test address"),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    let handle = Arc::new(handle);
    let reply_session =
        crate::AskContext::from_stream_handle_with_request_id(0, &handle, None, None)
            .reply_session()
            .unwrap();
    let delivery = budget.bind(&reply_session).unwrap();
    let responder =
        AskResponder::from_stream_handle(77, Arc::clone(&handle), Arc::new(AtomicBool::new(false)));
    let lease = responder
        .try_reply_lease(&delivery, 32)
        .expect("lease admission");
    drop(lease);

    let mut header = [0u8; crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN];
    peer.read_exact(&mut header)
        .await
        .expect("terminal response header");
    let control = crate::framing::decode_control(header[..4].try_into().expect("control"))
        .expect("response control");
    assert_eq!(control.kind, crate::framing::WireKind::Response);
    let payload_len = control
        .body_len
        .saturating_sub(crate::framing::ASK_RESPONSE_HEADER_LEN);
    let mut payload = vec![0u8; payload_len];
    peer.read_exact(&mut payload)
        .await
        .expect("terminal response payload");
    assert_eq!(payload, b"duplicate-suppressed");

    handle.shutdown();
    let _ = writer_task.await;
}

#[tokio::test]
async fn close_before_publish_is_rejected_without_fresh_responder() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Observer(AtomicUsize);
    impl AskReplyObserver for Observer {
        fn reply_claimed(&self, _payload: Bytes) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let budget =
        ReplyDeliveryBudget::new(2, 64, ReplyPayload::from_static(b"duplicate-suppressed"))
            .expect("valid reply budget");
    let (io, _peer) = tokio::io::duplex(4096);
    let (handle, writer_task, _reader_task) = LockFreeStreamHandle::new(
        io,
        "127.0.0.1:40568".parse().expect("test address"),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    let handle = Arc::new(handle);
    let reply_session =
        crate::AskContext::from_stream_handle_with_request_id(0, &handle, None, None)
            .reply_session()
            .unwrap();
    let delivery = budget.bind(&reply_session).unwrap();
    let observer = Arc::new(Observer(AtomicUsize::new(0)));
    let context = crate::AskContext::from_stream_handle_with_request_id(79, &handle, None, None)
        .with_reply_observer(Arc::clone(&observer) as Arc<dyn AskReplyObserver>);
    let lease = context
        .responder()
        .try_reply_lease(&delivery, 32)
        .expect("lease admission");
    handle.reply_slots().close_and_reclaim();

    let result = lease.try_reply_bytes(ReplyPayload::from_static(b"late"));
    assert!(matches!(
        result,
        Err(crate::GossipError::ConnectionClosed(addr))
            if addr == "127.0.0.1:40568".parse::<std::net::SocketAddr>().expect("address")
    ));
    assert_eq!(observer.0.load(Ordering::SeqCst), 0);
    assert_eq!(handle.reply_slots().reserved(), 0);
    let sibling_result = context
        .responder()
        .try_reply_bytes(crate::ForwardPayload::from_static(b"sibling"));
    assert!(matches!(
        sibling_result,
        Err(crate::GossipError::Network(error))
            if error.kind() == std::io::ErrorKind::AlreadyExists
    ));

    handle.shutdown();
    let _ = writer_task.await;
}

#[tokio::test]
async fn publication_close_public_publisher_first_notifies_once() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Observer(AtomicUsize);
    impl AskReplyObserver for Observer {
        fn reply_claimed(&self, _payload: Bytes) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn reply_claimed_borrowed(&self, payload: crate::ReplyPayloadRef<'_>) {
            assert_eq!(payload.prefix(), None);
            assert_eq!(payload.payload(), b"published");
            assert_eq!(payload.len(), b"published".len());
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let budget =
        ReplyDeliveryBudget::new(1, 32, ReplyPayload::from_static(b"duplicate-suppressed"))
            .expect("valid budget");
    let (io, _peer) = tokio::io::duplex(4096);
    let (handle, writer_task, _reader_task) = LockFreeStreamHandle::new(
        io,
        "127.0.0.1:40576".parse().expect("test address"),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    let handle = Arc::new(handle);
    let reply_session =
        crate::AskContext::from_stream_handle_with_request_id(0, &handle, None, None)
            .reply_session()
            .unwrap();
    let delivery = budget.bind(&reply_session).unwrap();
    let observer = Arc::new(Observer(AtomicUsize::new(0)));
    let context = crate::AskContext::from_stream_handle_with_request_id(80, &handle, None, None)
        .with_reply_observer(Arc::clone(&observer) as Arc<dyn AskReplyObserver>);
    let lease = context
        .responder()
        .try_reply_lease(&delivery, 32)
        .expect("lease admission");
    lease
        .try_reply_bytes(ReplyPayload::from_static(b"published"))
        .expect("public publication");
    assert_eq!(observer.0.load(Ordering::SeqCst), 1);
    handle.reply_slots().close_and_reclaim();
    assert_eq!(handle.reply_slots().stats_snapshot().reserved_jobs, 0);
    assert_eq!(
        handle
            .reply_slots()
            .stats_snapshot()
            .retained_payload_owners,
        0
    );
    assert_eq!(budget.available_test_permits(), (1, 32));
    handle.shutdown();
    let _ = writer_task.await;
}

#[tokio::test]
async fn completion_before_first_wait_poll_is_observed() {
    let slots = crate::connection_pool::reply_slots::ReplySlots::new(
        1,
        "127.0.0.1:40567".parse().expect("test address"),
        Arc::new(tokio::sync::Notify::new()),
    );
    let jobs = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(32));
    let record = slots
        .try_reserve(
            1,
            32,
            Arc::new(ReplyPayload::from_static(b"cancelled")),
            jobs.try_acquire_owned().expect("job permit"),
            bytes.try_acquire_many_owned(32).expect("byte permit"),
        )
        .expect("reserve");

    // The writer can finish a record before its consumer gets scheduled for
    // the first time. The wait must observe that completion without parking.
    record.complete();
    tokio::time::timeout(std::time::Duration::from_secs(1), record.wait_complete())
        .await
        .expect("completed reply slot must not wait forever")
        .expect("normal completion");
}

#[tokio::test]
async fn completion_releases_budget_before_reply_waiter_returns() {
    let slots = crate::connection_pool::reply_slots::ReplySlots::new(
        1,
        "127.0.0.1:40577".parse().expect("test address"),
        Arc::new(tokio::sync::Notify::new()),
    );
    let jobs = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(32));
    let record = slots
        .try_reserve(
            1,
            32,
            Arc::new(ReplyPayload::from_static(b"cancelled")),
            jobs.clone().try_acquire_owned().expect("job permit"),
            bytes
                .clone()
                .try_acquire_many_owned(32)
                .expect("byte permit"),
        )
        .expect("reserve");

    record.finish();
    assert_eq!(jobs.available_permits(), 1);
    assert_eq!(bytes.available_permits(), 32);
    assert_eq!(
        slots.free_count(),
        crate::connection_pool::reply_slots::REPLY_SLOT_CAP - 1
    );

    let replacement = slots
        .try_reserve(
            2,
            32,
            Arc::new(ReplyPayload::from_static(b"cancelled")),
            jobs.try_acquire_owned().expect("recycled job permit"),
            bytes
                .try_acquire_many_owned(32)
                .expect("recycled byte permit"),
        )
        .expect("a completed response must not hold the one-job budget");
    replacement.finish();
    record
        .wait_complete()
        .await
        .expect("normal completion must be observable");
    drop(record);
    drop(replacement);
    assert_eq!(slots.free_count(), super::REPLY_SLOT_CAP);
}

#[test]
fn close_before_publication_is_rejected_at_the_close_linearization_point() {
    let slots = crate::connection_pool::reply_slots::ReplySlots::new(
        1,
        "127.0.0.1:40569".parse().expect("test address"),
        Arc::new(tokio::sync::Notify::new()),
    );
    let jobs = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(32));
    let record = slots
        .try_reserve(
            1,
            32,
            Arc::new(ReplyPayload::from_static(b"cancelled")),
            jobs.try_acquire_owned().expect("job permit"),
            bytes.try_acquire_many_owned(32).expect("byte permit"),
        )
        .expect("reserve");

    slots.close_and_reclaim();
    let publish = record.publish(ReplyPayload::from_static(b"reply"));
    assert!(matches!(
        publish,
        Err(crate::GossipError::ConnectionClosed(_))
    ));
    assert_eq!(slots.reserved(), 0);
}

#[test]
fn publication_close_race_has_no_second_publication() {
    let slots = crate::connection_pool::reply_slots::ReplySlots::new(
        1,
        "127.0.0.1:40569".parse().expect("test address"),
        Arc::new(tokio::sync::Notify::new()),
    );
    let jobs = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(32));
    let record = slots
        .try_reserve(
            1,
            32,
            Arc::new(ReplyPayload::from_static(b"cancelled")),
            jobs.try_acquire_owned().expect("job permit"),
            bytes.try_acquire_many_owned(32).expect("byte permit"),
        )
        .expect("reserve");

    let publish = record.publish(ReplyPayload::from_static(b"reply"));
    slots.close_and_reclaim();
    let late_publish = record.publish(ReplyPayload::from_static(b"again"));
    assert!(publish.is_ok(), "publication precedes the explicit close");
    assert!(matches!(
        late_publish,
        Err(crate::GossipError::ConnectionClosed(_)) | Err(crate::GossipError::Network(_))
    ));
    assert_eq!(slots.reserved(), 0);
}

#[test]
fn publication_close_controlled_overlap_has_one_linearized_result() {
    let slots = crate::connection_pool::reply_slots::ReplySlots::new(
        1,
        "127.0.0.1:40571".parse().expect("test address"),
        Arc::new(tokio::sync::Notify::new()),
    );
    let jobs = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(32));
    let record = slots
        .try_reserve(
            1,
            32,
            Arc::new(ReplyPayload::from_static(b"cancelled")),
            jobs.try_acquire_owned().expect("job permit"),
            bytes.try_acquire_many_owned(32).expect("byte permit"),
        )
        .expect("reserve");
    let gate = record.publication_test_gate();
    gate.arm();
    let publisher_record = record.clone();
    let publisher =
        std::thread::spawn(move || publisher_record.publish(ReplyPayload::from_static(b"reply")));
    gate.wait_publisher_entered();

    let close_slots = Arc::clone(&slots);
    let closer = std::thread::spawn(move || {
        close_slots.close_and_reclaim();
    });
    gate.wait_close_observed();
    gate.release();

    let publication = publisher.join().expect("publisher must not panic");
    closer.join().expect("closer must not panic");
    assert!(
        publication.is_ok() || matches!(publication, Err(crate::GossipError::ConnectionClosed(_))),
        "close/publication must have one linearized outcome: {publication:?}"
    );
    let late = record.publish(ReplyPayload::from_static(b"again"));
    assert!(
        matches!(late, Err(crate::GossipError::ConnectionClosed(_)))
            || matches!(late, Err(crate::GossipError::Network(_))),
        "a close race must never permit a second publication: {late:?}"
    );
    drop(record);
    let stats = slots.stats_snapshot();
    assert_eq!(stats.discarded_reservations, 0);
    assert_eq!(stats.normal_completions, 0);
    assert_eq!(stats.terminal_completions, 0);
    assert_eq!(stats.reserved_jobs, 0);
    assert_eq!(stats.reserved_bytes, 0);
}

#[test]
fn reservation_accounting_after_permit_release() {
    let slots = crate::connection_pool::reply_slots::ReplySlots::new(
        1,
        "127.0.0.1:40575".parse().expect("test address"),
        Arc::new(tokio::sync::Notify::new()),
    );
    let jobs = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(32));
    let record = slots
        .try_reserve(
            1,
            32,
            Arc::new(ReplyPayload::from_static(b"terminal")),
            jobs.clone().try_acquire_owned().expect("job permit"),
            bytes
                .clone()
                .try_acquire_many_owned(32)
                .expect("byte permit"),
        )
        .expect("reservation");

    record.finish();
    drop(record);

    let stats = slots.stats_snapshot();
    assert_eq!(jobs.available_permits(), 1);
    assert_eq!(bytes.available_permits(), 32);
    assert_eq!(stats.reserved_jobs, 0);
    assert_eq!(stats.reserved_bytes, 0);
    assert!(
        !slots
            .stats
            .accounting_before_permits
            .load(std::sync::atomic::Ordering::Acquire),
        "reservation accounting must follow both permit releases"
    );
}

#[test]
fn payload_destruction_before_capacity_release() {
    use std::sync::atomic::{AtomicBool, Ordering};

    struct PayloadOwner {
        bytes: Vec<u8>,
        dropped_before_capacity_release: Arc<AtomicBool>,
        jobs: Arc<Semaphore>,
        bytes_permits: Arc<Semaphore>,
        slots: Arc<crate::connection_pool::reply_slots::ReplySlots>,
    }

    impl AsRef<[u8]> for PayloadOwner {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for PayloadOwner {
        fn drop(&mut self) {
            if self.jobs.available_permits() == 0
                && self.bytes_permits.available_permits() == 0
                && self.slots.free_count()
                    == crate::connection_pool::reply_slots::REPLY_SLOT_CAP - 1
            {
                self.dropped_before_capacity_release
                    .store(true, Ordering::Release);
            }
        }
    }

    let slots = crate::connection_pool::reply_slots::ReplySlots::new(
        1,
        "127.0.0.1:40574".parse().expect("test address"),
        Arc::new(tokio::sync::Notify::new()),
    );
    let jobs = Arc::new(Semaphore::new(1));
    let bytes_permits = Arc::new(Semaphore::new(32));
    let normal_dropped = Arc::new(AtomicBool::new(false));
    let terminal_dropped = Arc::new(AtomicBool::new(false));
    let normal_owner = PayloadOwner {
        bytes: vec![7; 8],
        dropped_before_capacity_release: Arc::clone(&normal_dropped),
        jobs: Arc::clone(&jobs),
        bytes_permits: Arc::clone(&bytes_permits),
        slots: Arc::clone(&slots),
    };
    let terminal_owner = PayloadOwner {
        bytes: b"cancelled".to_vec(),
        dropped_before_capacity_release: Arc::clone(&terminal_dropped),
        jobs: Arc::clone(&jobs),
        bytes_permits: Arc::clone(&bytes_permits),
        slots: Arc::clone(&slots),
    };
    let terminal_retained = terminal_owner.bytes.capacity();
    let normal_retained = normal_owner.bytes.capacity();
    let record = slots
        .try_reserve(
            1,
            32,
            Arc::new(crate::ReplyPayload::from_owner(
                terminal_owner,
                terminal_retained,
            )),
            Arc::clone(&jobs).try_acquire_owned().expect("job permit"),
            Arc::clone(&bytes_permits)
                .try_acquire_many_owned(32)
                .expect("byte permit"),
        )
        .expect("reservation");
    record
        .publish(crate::ReplyPayload::from_owner(
            normal_owner,
            normal_retained,
        ))
        .expect("publication");
    record.finish();
    drop(record);

    assert!(
        normal_dropped.load(Ordering::Acquire),
        "normal payload must be destroyed while permits are held and before slot recycle"
    );
    assert!(
        terminal_dropped.load(Ordering::Acquire),
        "terminal payload must be destroyed while permits are held and before slot recycle"
    );
    assert_eq!(jobs.available_permits(), 1);
    assert_eq!(bytes_permits.available_permits(), 32);
    assert_eq!(
        slots.free_count(),
        crate::connection_pool::reply_slots::REPLY_SLOT_CAP
    );
}

#[tokio::test]
async fn published_lease_releases_after_terminal_flush() {
    use tokio::io::AsyncReadExt;

    let budget =
        ReplyDeliveryBudget::new(1, 32, ReplyPayload::from_static(b"duplicate-suppressed"))
            .expect("valid reply budget");
    let (io, mut peer) = tokio::io::duplex(4096);
    let (handle, writer_task, _reader_task) = LockFreeStreamHandle::new(
        io,
        "127.0.0.1:40566".parse().expect("test address"),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    let handle = Arc::new(handle);
    let reply_session =
        crate::AskContext::from_stream_handle_with_request_id(0, &handle, None, None)
            .reply_session()
            .unwrap();
    let delivery = budget.bind(&reply_session).unwrap();
    let responder =
        AskResponder::from_stream_handle(78, Arc::clone(&handle), Arc::new(AtomicBool::new(false)));
    let lease = responder
        .try_reply_lease(&delivery, 32)
        .expect("lease admission");
    lease
        .try_reply_bytes(ReplyPayload::copy_from_slice(b"reply"))
        .expect("publish to the IO owner");

    let mut header = [0u8; crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN];
    peer.read_exact(&mut header).await.expect("response header");
    let control = crate::framing::decode_control(header[..4].try_into().expect("control"))
        .expect("response control");
    let payload_len = control
        .body_len
        .saturating_sub(crate::framing::ASK_RESPONSE_HEADER_LEN);
    let mut payload = vec![0u8; payload_len];
    peer.read_exact(&mut payload)
        .await
        .expect("response payload");
    assert_eq!(payload, b"reply");

    let next =
        AskResponder::from_stream_handle(79, Arc::clone(&handle), Arc::new(AtomicBool::new(false)))
            .try_reply_lease(&delivery, 32)
            .expect("terminal flush must release the budget and slot");
    drop(next);
    handle.shutdown();
    let _ = writer_task.await;
}

#[test]
fn close_does_not_spin_on_a_preempted_reply_publisher() {
    let slots = crate::connection_pool::reply_slots::ReplySlots::new(
        1,
        "127.0.0.1:40579".parse().unwrap(),
        Arc::new(tokio::sync::Notify::new()),
    );
    let jobs = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(32));
    let record = slots
        .try_reserve(
            1,
            32,
            Arc::new(ReplyPayload::from_static(b"cancelled")),
            jobs.clone().try_acquire_owned().unwrap(),
            bytes.clone().try_acquire_many_owned(32).unwrap(),
        )
        .unwrap();
    let gate = record.publication_test_gate();
    gate.arm();
    let publisher_record = record.clone();
    let publisher =
        std::thread::spawn(move || publisher_record.publish(ReplyPayload::from_static(b"reply")));
    gate.wait_publisher_entered();
    let close_slots = slots.clone();
    let (sent, received) = std::sync::mpsc::sync_channel(1);
    let closer = std::thread::spawn(move || {
        close_slots.close_and_reclaim();
        sent.send(()).unwrap();
    });
    gate.wait_close_observed();
    let closed_without_waiting = received
        .recv_timeout(std::time::Duration::from_secs(1))
        .is_ok();
    let charged_during_publication =
        jobs.available_permits() == 0 && bytes.available_permits() == 0;
    gate.release();
    let _ = publisher.join().unwrap();
    closer.join().unwrap();
    assert!(
        closed_without_waiting,
        "close must not wait for the publisher"
    );
    assert!(
        charged_during_publication,
        "exclusive publisher retains its permits"
    );
    assert_eq!(jobs.available_permits(), 1);
    assert_eq!(bytes.available_permits(), 32);
    assert!(matches!(
        record.publish(ReplyPayload::from_static(b"late")),
        Err(crate::GossipError::ConnectionClosed(_))
    ));
}

#[test]
fn reservation_close_hands_drain_to_the_last_publisher() {
    let slots = crate::connection_pool::reply_slots::ReplySlots::new(
        1,
        "127.0.0.1:40580".parse().unwrap(),
        Arc::new(tokio::sync::Notify::new()),
    );
    assert!(slots.begin_reservation());
    let closing = slots.clone();
    let (sent, received) = std::sync::mpsc::sync_channel(1);
    let closer = std::thread::spawn(move || {
        closing.close_and_reclaim();
        sent.send(()).unwrap();
    });
    let closed_without_waiting = received
        .recv_timeout(std::time::Duration::from_secs(1))
        .is_ok();
    assert!(
        !slots.begin_reservation(),
        "closed owner rejects new publishers"
    );
    slots.end_reservation();
    closer.join().unwrap();
    assert!(
        closed_without_waiting,
        "close must transfer drain ownership"
    );
    assert!(
        slots
            .close_drained
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert_eq!(
        slots
            .reservation_state
            .load(std::sync::atomic::Ordering::Acquire),
        super::RESERVATION_CLOSED
    );
}

#[test]
fn complete_and_discard_defer_reclamation_until_publication_finishes() {
    for discard in [false, true] {
        let slots = crate::connection_pool::reply_slots::ReplySlots::new(
            1,
            "127.0.0.1:40581".parse().unwrap(),
            Arc::new(tokio::sync::Notify::new()),
        );
        let jobs = Arc::new(Semaphore::new(1));
        let bytes = Arc::new(Semaphore::new(32));
        let record = slots
            .try_reserve(
                1,
                32,
                Arc::new(ReplyPayload::from_static(b"cancelled")),
                jobs.clone().try_acquire_owned().unwrap(),
                bytes.clone().try_acquire_many_owned(32).unwrap(),
            )
            .unwrap();
        let gate = record.publication_test_gate();
        gate.arm();
        let publishing = record.clone();
        let publisher =
            std::thread::spawn(move || publishing.publish(ReplyPayload::from_static(b"reply")));
        gate.wait_publisher_entered();
        let completing = record.clone();
        let (sent, received) = std::sync::mpsc::sync_channel(1);
        let completion = std::thread::spawn(move || {
            if discard {
                completing.discard();
            } else {
                completing.complete();
            }
            sent.send(()).unwrap();
        });
        let returned = received
            .recv_timeout(std::time::Duration::from_secs(1))
            .is_ok();
        let still_charged = jobs.available_permits() == 0
            && bytes.available_permits() == 0
            && record.state.load(std::sync::atomic::Ordering::Acquire) == super::STATE_RESERVED;
        gate.release();
        let _ = publisher.join().unwrap();
        completion.join().unwrap();
        assert!(
            returned,
            "finalization must not spin waiting for publication"
        );
        assert!(
            still_charged,
            "completion cannot precede owner/permit release"
        );
        assert_eq!(jobs.available_permits(), 1);
        assert_eq!(bytes.available_permits(), 32);
        assert!(record.normal_payload().is_none());
    }
}

#[test]
fn inline_publication_has_one_consumer_and_preserves_owner() {
    let cell = super::ReplyPublication::new();
    let payload = ReplyPayload::from_static(b"inline-owned-reply");
    assert!(cell.pop().is_none(), "unpublished cell is not readable");
    cell.publish(payload);
    std::thread::scope(|scope| {
        let first = scope.spawn(|| cell.pop());
        let second = scope.spawn(|| cell.pop());
        let first = first.join().unwrap();
        let second = second.join().unwrap();
        assert_eq!(
            usize::from(first.is_some()) + usize::from(second.is_some()),
            1
        );
        assert_eq!(first.or(second).unwrap().len(), b"inline-owned-reply".len());
    });
    assert!(
        cell.pop().is_none(),
        "a consumed owner cannot be read twice"
    );
}

#[test]
fn preallocated_cells_wait_for_last_observer_before_generation_reuse() {
    let slots = super::ReplySlots::new(
        1,
        "127.0.0.1:40693".parse().unwrap(),
        Arc::new(tokio::sync::Notify::new()),
    );
    let jobs = Arc::new(Semaphore::new(super::REPLY_SLOT_CAP));
    let bytes = Arc::new(Semaphore::new(super::REPLY_SLOT_CAP * 8));
    let terminal = Arc::new(ReplyPayload::from_static(b"cancel"));
    let reserve = |id| {
        slots.try_reserve(
            id,
            8,
            terminal.clone(),
            jobs.clone().try_acquire_owned().unwrap(),
            bytes.clone().try_acquire_many_owned(8).unwrap(),
        )
    };
    let mut observers = Vec::with_capacity(super::REPLY_SLOT_CAP);
    for id in 0..super::REPLY_SLOT_CAP {
        let record = reserve(id as u32).unwrap();
        record.finish();
        observers.push(record);
    }
    assert_eq!(slots.free_count(), 0);
    assert_eq!(jobs.available_permits(), super::REPLY_SLOT_CAP);
    assert_eq!(bytes.available_permits(), super::REPLY_SLOT_CAP * 8);
    assert!(
        reserve(100).is_err(),
        "retained observers bound arena reuse"
    );
    let old = observers.pop().unwrap();
    let old_key = old.key();
    let storage = &slots.table[old.index] as *const super::ReplySlotCell;
    let last_observer = old.clone();
    drop(old);
    assert!(
        reserve(101).is_err(),
        "one remaining observer still pins storage"
    );
    drop(last_observer);
    assert_eq!(slots.free_count(), 1);
    let successor = reserve(102).unwrap();
    assert_eq!(
        &slots.table[successor.index] as *const super::ReplySlotCell,
        storage
    );
    assert_eq!(successor.key().stream_id, old_key.stream_id);
    assert!(successor.key().generation > old_key.generation);
    assert_eq!(successor.correlation_id(), 102);
    assert_eq!(
        successor.state.load(std::sync::atomic::Ordering::Acquire),
        super::STATE_RESERVED
    );
    successor.finish();
    drop(successor);
    drop(observers);
    assert_eq!(slots.free_count(), super::REPLY_SLOT_CAP);
}

#[test]
fn final_rejected_table_probe_owns_cell_reclamation() {
    use std::sync::atomic::Ordering;

    let slots = super::ReplySlots::new(
        1,
        "127.0.0.1:40694".parse().unwrap(),
        Arc::new(tokio::sync::Notify::new()),
    );
    let jobs = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(8));
    let record = slots
        .try_reserve(
            1,
            8,
            Arc::new(ReplyPayload::from_static(b"cancel")),
            jobs.clone().try_acquire_owned().unwrap(),
            bytes.clone().try_acquire_many_owned(8).unwrap(),
        )
        .unwrap();
    let index = record.index;
    let cell = &slots.table[index];
    record.finish();
    // Pause acquire after its increment observes a retired table. This probe
    // must never dereference the record, but temporarily delays destruction.
    let previous = cell.ownership.fetch_add(1, Ordering::AcqRel);
    assert_eq!(previous, super::CELL_ACTIVE | 1);
    drop(record);
    assert_eq!(slots.free_count(), super::REPLY_SLOT_CAP - 1);
    assert_eq!(
        cell.ownership.load(Ordering::Acquire),
        super::CELL_ACTIVE | 1
    );
    cell.release(&slots, index);
    assert_eq!(cell.ownership.load(Ordering::Acquire), 0);
    assert_eq!(slots.free_count(), super::REPLY_SLOT_CAP);
    assert!(
        cell.acquire(&slots, index).is_none(),
        "free storage has no readable record"
    );
    assert_eq!(slots.free_count(), super::REPLY_SLOT_CAP);
    assert_eq!(jobs.available_permits(), 1);
    assert_eq!(bytes.available_permits(), 8);
}

#[test]
fn publication_charges_complete_owner_and_preserves_retry_after_overload() {
    let slots = super::ReplySlots::new(
        1,
        "127.0.0.1:40695".parse().unwrap(),
        Arc::new(tokio::sync::Notify::new()),
    );
    let jobs = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(32));
    let record = slots
        .try_reserve(
            1,
            32,
            Arc::new(ReplyPayload::from_static(b"cancel")),
            jobs.clone().try_acquire_owned().unwrap(),
            bytes.clone().try_acquire_many_owned(32).unwrap(),
        )
        .unwrap();
    let mut oversized = Vec::with_capacity(4096);
    oversized.push(7);
    let retained = oversized.capacity();
    assert!(retained > 32 && oversized.len() < 32);
    let error = record.publish(ReplyPayload::from(oversized)).unwrap_err();
    assert!(matches!(
        error,
        crate::GossipError::MessageTooLarge { size, max: 32 } if size == retained
    ));
    assert!(record.normal_payload().is_none());
    assert_eq!(jobs.available_permits(), 0);
    assert_eq!(bytes.available_permits(), 0);
    record.publish(ReplyPayload::from_static(b"valid")).unwrap();
    assert_eq!(record.normal_payload().unwrap().as_ref(), b"valid");
    record.finish();
    drop(record);
    assert_eq!(jobs.available_permits(), 1);
    assert_eq!(bytes.available_permits(), 32);
    assert_eq!(slots.free_count(), super::REPLY_SLOT_CAP);
}

#[test]
fn terminal_owner_moves_once_without_incrementing_shared_reference_count() {
    let slots = super::ReplySlots::new(
        1,
        "127.0.0.1:40696".parse().unwrap(),
        Arc::new(tokio::sync::Notify::new()),
    );
    let jobs = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(32));
    let terminal = Arc::new(ReplyPayload::from_static(b"cancel"));
    let record = slots
        .try_reserve(
            1,
            32,
            terminal.clone(),
            jobs.clone().try_acquire_owned().unwrap(),
            bytes.clone().try_acquire_many_owned(32).unwrap(),
        )
        .unwrap();
    let references = Arc::strong_count(&terminal);
    let selected = record.take_terminal_payload().unwrap();
    assert!(Arc::ptr_eq(&selected, &terminal));
    assert_eq!(Arc::strong_count(&terminal), references);
    assert!(record.take_terminal_payload().is_none());
    assert_eq!(jobs.available_permits(), 0);
    drop(selected);
    assert_eq!(Arc::strong_count(&terminal), 1);
    record.finish();
    drop(record);
    assert_eq!(jobs.available_permits(), 1);
    assert_eq!(bytes.available_permits(), 32);
}

#[test]
fn writer_exit_waits_for_final_close_publisher_without_racing_ready_drain() {
    use std::sync::atomic::Ordering;

    let slots = super::ReplySlots::new(
        91,
        "127.0.0.1:40701".parse().unwrap(),
        Arc::new(tokio::sync::Notify::new()),
    );
    let owner = slots.writer_owner().unwrap();
    let jobs = Arc::new(Semaphore::new(2));
    let bytes = Arc::new(Semaphore::new(64));
    let reserve = |id| {
        slots
            .try_reserve(
                id,
                32,
                Arc::new(ReplyPayload::from_static(b"cancel")),
                jobs.clone().try_acquire_owned().unwrap(),
                bytes.clone().try_acquire_many_owned(32).unwrap(),
            )
            .unwrap()
    };
    let active = reserve(1);
    active
        .publish(ReplyPayload::from_static(b"active"))
        .unwrap();
    active.activate();
    let abandoned = slots.pop_ready().unwrap();
    let staged_payload = owner.record(&abandoned).unwrap().normal_payload().unwrap();
    let queued = reserve(2);
    queued.activate();
    assert_eq!(
        slots.ready.len(),
        1,
        "control includes an unpopped writer token"
    );
    assert!(
        slots.begin_reservation(),
        "pause a final publication before close"
    );
    std::thread::scope(|scope| {
        let closing = scope.spawn(|| slots.close_and_reclaim());
        closing.join().unwrap();
        drop(staged_payload);
        let exiting = scope.spawn(move || drop(owner));
        exiting.join().unwrap();
    });
    assert!(!slots.close_done.load(Ordering::Acquire));
    assert!(!slots.writer_exit_drained.load(Ordering::Acquire));
    assert_eq!(jobs.available_permits(), 0);
    assert_eq!(bytes.available_permits(), 0);
    slots.end_reservation();
    assert!(slots.close_done.load(Ordering::Acquire));
    assert!(slots.writer_exit_drained.load(Ordering::Acquire));
    assert_eq!(slots.ready.len(), 0);
    assert_eq!(slots.reserved(), 0);
    assert_eq!(jobs.available_permits(), 2);
    assert_eq!(bytes.available_permits(), 64);
    assert_eq!(slots.stats_snapshot().reserved_jobs, 0);
    assert!(
        slots.writer_owner().is_err(),
        "a successor IO owner cannot reuse this arena"
    );
    slots.close_and_reclaim();
    drop(active);
    drop(queued);
    assert_eq!(
        jobs.available_permits(),
        2,
        "cleanup must happen exactly once"
    );
}

#[test]
fn ordinary_close_keeps_staged_writer_payload_charged_until_owner_acknowledges() {
    let slots = super::ReplySlots::new(
        92,
        "127.0.0.1:40702".parse().unwrap(),
        Arc::new(tokio::sync::Notify::new()),
    );
    let owner = slots.writer_owner().unwrap();
    let jobs = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(32));
    let record = slots
        .try_reserve(
            1,
            32,
            Arc::new(ReplyPayload::from_static(b"cancel")),
            jobs.clone().try_acquire_owned().unwrap(),
            bytes.clone().try_acquire_many_owned(32).unwrap(),
        )
        .unwrap();
    record.publish(ReplyPayload::from_static(b"owned")).unwrap();
    record.activate();
    let token = slots.pop_ready().unwrap();
    let payload = owner.record(&token).unwrap().normal_payload().unwrap();
    slots.close_and_reclaim();
    assert_eq!(jobs.available_permits(), 0);
    assert_eq!(bytes.available_permits(), 0);
    assert_eq!(payload.as_ref(), b"owned");
    assert!(
        owner.record(&token).is_some(),
        "close preserves the writer lifetime"
    );
    drop(payload);
    owner.finish(token);
    assert_eq!(jobs.available_permits(), 1);
    assert_eq!(bytes.available_permits(), 32);
    assert_eq!(slots.reserved(), 0);
}

#[test]
fn observer_unwind_drops_original_payload_before_close_releases_budget() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TrackedOwner {
        bytes: Vec<u8>,
        jobs: Arc<Semaphore>,
        budget: Arc<Semaphore>,
        dropped: Arc<AtomicUsize>,
    }

    impl AsRef<[u8]> for TrackedOwner {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for TrackedOwner {
        fn drop(&mut self) {
            assert_eq!(self.jobs.available_permits(), 0);
            assert_eq!(self.budget.available_permits(), 0);
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    let slots = super::ReplySlots::new(
        94,
        "127.0.0.1:40706".parse().unwrap(),
        Arc::new(tokio::sync::Notify::new()),
    );
    let jobs = Arc::new(Semaphore::new(1));
    let bytes = Arc::new(Semaphore::new(32));
    let dropped = Arc::new(AtomicUsize::new(0));
    let record = slots
        .try_reserve(
            1,
            32,
            Arc::new(ReplyPayload::from_static(b"cancel")),
            jobs.clone().try_acquire_owned().unwrap(),
            bytes.clone().try_acquire_many_owned(32).unwrap(),
        )
        .unwrap();
    let payload = ReplyPayload::from_owner(
        TrackedOwner {
            bytes: b"original".to_vec(),
            jobs: Arc::clone(&jobs),
            budget: Arc::clone(&bytes),
            dropped: Arc::clone(&dropped),
        },
        8,
    );
    let pointer = payload.as_ref().as_ptr();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        record.publish_observed(payload, |borrowed| {
            assert_eq!(borrowed.as_ref().as_ptr(), pointer);
            slots.close_and_reclaim();
            assert_eq!(jobs.available_permits(), 0);
            panic!("controlled observer unwind");
        })
    }));
    assert!(result.is_err());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(jobs.available_permits(), 1);
    assert_eq!(bytes.available_permits(), 32);
    assert_eq!(slots.stats_snapshot().reserved_jobs, 0);
    assert_eq!(slots.stats_snapshot().retained_payload_owners, 0);
}

#[test]
fn writer_tokens_cannot_target_another_arena_with_the_same_test_instance_id() {
    let first = super::ReplySlots::new(
        95,
        "127.0.0.1:40707".parse().unwrap(),
        Arc::new(tokio::sync::Notify::new()),
    );
    let other = super::ReplySlots::new(
        95,
        "127.0.0.1:40707".parse().unwrap(),
        Arc::new(tokio::sync::Notify::new()),
    );
    let first_owner = first.writer_owner().unwrap();
    let other_owner = other.writer_owner().unwrap();
    let jobs = Arc::new(Semaphore::new(2));
    let bytes = Arc::new(Semaphore::new(64));
    let reserve = |slots: &Arc<super::ReplySlots>| {
        slots
            .try_reserve(
                1,
                32,
                Arc::new(ReplyPayload::from_static(b"cancel")),
                jobs.clone().try_acquire_owned().unwrap(),
                bytes.clone().try_acquire_many_owned(32).unwrap(),
            )
            .unwrap()
    };
    let first_record = reserve(&first);
    let other_record = reserve(&other);
    first_record.activate();
    other_record.activate();
    let first_token = first.pop_ready().unwrap();
    let other_token = other.pop_ready().unwrap();
    assert!(other_owner.record(&first_token).is_none());
    assert!(first_owner.record(&other_token).is_none());
    assert!(first_owner.record(&first_token).is_some());
    assert!(other_owner.record(&other_token).is_some());
    assert_eq!(jobs.available_permits(), 0);
    first_owner.finish(first_token);
    assert_eq!(jobs.available_permits(), 1);
    assert!(other_owner.record(&other_token).is_some());
    other_owner.finish(other_token);
    assert_eq!(jobs.available_permits(), 2);
    assert_eq!(bytes.available_permits(), 64);
}
