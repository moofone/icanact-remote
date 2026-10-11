use icanact_remote::{ReplyDeliveryBudget, ReplyPayload};

#[test]
fn reply_payload_is_exact_sized_and_budget_rejects_zero_limits() {
    let payload = ReplyPayload::copy_from_slice(b"terminal").into_shared();
    assert_eq!(payload.len(), 8);
    assert_eq!(payload.as_ref(), b"terminal");
    assert!(
        ReplyDeliveryBudget::new(0, 1, payload.shared_clone().expect("shared payload")).is_err()
    );
    assert!(ReplyDeliveryBudget::new(1, 0, payload).is_err());
    assert!(
        ReplyDeliveryBudget::new(usize::MAX, 1, ReplyPayload::from_static(b"terminal"),).is_err()
    );
}

#[cfg(feature = "test-helpers")]
#[tokio::test]
async fn rejected_claim_has_no_wire_bytes_and_releases_capacity() {
    use icanact_remote::lease_test_support::{BufferConfig, ChannelId, LockFreeStreamHandle};
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    let budget =
        ReplyDeliveryBudget::new(2, 64, ReplyPayload::from_static(b"duplicate-suppressed"))
            .unwrap();
    let (io, mut peer) = tokio::io::duplex(4096);
    let (handle, writer_task, _reader_task) = LockFreeStreamHandle::new(
        io,
        "127.0.0.1:40567".parse().unwrap(),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    let handle = Arc::new(handle);
    let context = icanact_remote::AskContext::from_stream_handle_for_test(80, &handle);
    let reply_session = context.reply_session().expect("reply session");
    let delivery = budget.bind(&reply_session).expect("delivery session");
    let first = context.responder().try_reply_lease(&delivery, 32).unwrap();
    let rejected = context
        .responder()
        .try_reply_lease(&delivery, 32)
        .unwrap_err();
    assert!(matches!(
        rejected,
        icanact_remote::ReplyLeaseAdmissionError::ClaimUnavailable(_)
    ));
    let mut bytes = [0u8; 16];
    let read = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        peer.read_exact(&mut bytes),
    )
    .await;
    assert!(read.is_err());

    let replacement = icanact_remote::AskContext::from_stream_handle_for_test(81, &handle)
        .responder()
        .try_reply_lease(&delivery, 32)
        .expect("discarded sibling reservation must release capacity");
    replacement
        .try_reply_bytes(ReplyPayload::from_static(b"replacement"))
        .unwrap();
    drop(first);
    handle.shutdown();
    let _ = writer_task.await;
}

#[cfg(feature = "test-helpers")]
#[tokio::test]
async fn closed_sync_lease_publication_rejects_after_real_transport_exit() {
    use icanact_remote::lease_test_support::{BufferConfig, ChannelId, LockFreeStreamHandle};
    use std::sync::Arc;

    let budget =
        ReplyDeliveryBudget::new(1, 32, ReplyPayload::from_static(b"duplicate-suppressed"))
            .expect("valid budget");
    let (io, _peer) = tokio::io::duplex(4096);
    let (handle, writer_task, _reader_task) = LockFreeStreamHandle::new(
        io,
        "127.0.0.1:40571".parse().expect("test address"),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    let handle = Arc::new(handle);
    let context = icanact_remote::AskContext::from_stream_handle_for_test(82, &handle);
    let reply_session = context.reply_session().expect("reply session");
    let delivery = budget.bind(&reply_session).expect("delivery session");
    let lease = context
        .responder()
        .try_reply_lease(&delivery, 32)
        .expect("lease admission");

    handle.shutdown();
    handle.wait_for_exit().await;
    let error = lease
        .try_reply_bytes(ReplyPayload::from_static(b"late"))
        .expect_err("closed transport must reject synchronous publication");
    assert!(matches!(
        error,
        icanact_remote::GossipError::ConnectionClosed(_)
    ));
    let _ = writer_task.await;
}
