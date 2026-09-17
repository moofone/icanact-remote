use std::sync::{Arc, atomic::AtomicBool};

use bytes::Bytes;

use crate::connection_pool::{BufferConfig, ChannelId, LockFreeStreamHandle};
use crate::{AskResponder, ReplyDeliveryBudget, ReplyPayload};

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

    let mut leases = Vec::with_capacity(64);
    for correlation_id in 0..64 {
        let responder = AskResponder::from_stream_handle(
            correlation_id,
            Arc::clone(&handle),
            Arc::new(AtomicBool::new(false)),
        );
        leases.push(
            responder
                .try_reply_lease(&budget, Bytes::from_static(b"reply").len())
                .expect("each reserved slot admits one responder"),
        );
    }

    let responder =
        AskResponder::from_stream_handle(64, Arc::clone(&handle), Arc::new(AtomicBool::new(false)));
    let error = responder
        .try_reply_lease(&budget, 5)
        .expect_err("the bounded connection slot set is exhausted");
    let responder = error
        .into_responder()
        .expect("admission failure preserves responder");
    assert!(
        !responder
            .try_reply_bytes(Bytes::from_static(b"still-owned"))
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
    let responder =
        AskResponder::from_stream_handle(77, Arc::clone(&handle), Arc::new(AtomicBool::new(false)));
    let lease = responder
        .try_reply_lease(&budget, 32)
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
    let responder =
        AskResponder::from_stream_handle(78, Arc::clone(&handle), Arc::new(AtomicBool::new(false)));
    let lease = responder
        .try_reply_lease(&budget, 32)
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
            .try_reply_lease(&budget, 32)
            .expect("terminal flush must release the budget and slot");
    drop(next);
    handle.shutdown();
    let _ = writer_task.await;
}
