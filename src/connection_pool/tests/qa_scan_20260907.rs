#[tokio::test(flavor = "current_thread")]
async fn qa_scan_pooled_writer_observes_shutdown() {
    let (stream, mut peer) = tokio::io::duplex(64);
    let (writer, mut task, _) = LockFreeStreamHandle::new(
        stream,
        "127.0.0.1:39920".parse().unwrap(),
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        None,
    );
    let payload =
        crate::typed::PooledPayload::try_from_pooled_bytes(8192, |b| b.resize(8192, 7)).unwrap();
    let header =
        crate::framing::try_write_ask_response_header(crate::MessageType::Response, 1, 8192)
            .unwrap();
    writer
        .write_pooled_control_inline_nonblocking(header, 16, None, 0, payload)
        .unwrap();
    let mut first = [0; 1];
    tokio::time::timeout(Duration::from_secs(2), peer.read_exact(&mut first))
        .await
        .unwrap()
        .unwrap();
    writer.shutdown();
    let exited = tokio::time::timeout(Duration::from_secs(2), &mut task)
        .await
        .is_ok();
    eprintln!("pooled queued writer: shutdown_completed_within_2s={exited}");
    if !exited {
        task.abort();
        let _ = task.await;
    }
    assert!(
        exited,
        "queued pooled writes must observe shutdown when peer stops reading"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn qa_scan_deferred_deadline_does_not_restart_at_wait() {
    let (io, peer) = tokio::io::duplex(4096);
    let (stream, task, _) = LockFreeStreamHandle::new(
        io,
        "127.0.0.1:39921".parse().unwrap(),
        ChannelId::Global,
        BufferConfig::default(),
        None,
        None,
    );
    let stream = Arc::new(stream);
    let conn = ConnectionHandle::<()>::new_stream(
        "127.0.0.1:39921".parse().unwrap(),
        ConnectionDirection::Outbound,
        stream.clone(),
        CorrelationTracker::new(),
    );
    let pending = conn
        .ask_deferred_with_timeout_bytes(
            bytes::Bytes::from_static(b"x"),
            Duration::from_millis(100),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let started = Instant::now();
    let outcome = pending.wait().await;
    let elapsed = started.elapsed();
    eprintln!("deferred: requested=100ms held=150ms extra_wait={elapsed:?} outcome={outcome:?}");
    stream.shutdown();
    task.abort();
    let _ = task.await;
    drop(peer);
    assert!(matches!(outcome, Err(GossipError::Timeout)));
    assert!(
        elapsed < Duration::from_millis(30),
        "already expired submission must time out immediately"
    );
}

struct QaHiddenOwner {
    data: Vec<u8>,
    live: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for QaHiddenOwner {
    fn as_ref(&self) -> &[u8] {
        &self.data[..1024]
    }
}
impl Drop for QaHiddenOwner {
    fn drop(&mut self) {
        self.live.fetch_sub(self.data.len(), Ordering::SeqCst);
    }
}
struct QaHiddenOwnerHandler {
    live: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
}
impl crate::registry::ActorAskImmediateHandlerSync for QaHiddenOwnerHandler {
    fn handle_actor_ask_sync_immediate(
        &self,
        _: u64,
        _: u32,
        _: crate::AlignedBytes,
    ) -> crate::Result<crate::registry::AskDisposition> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.live.fetch_add(1024 * 1024, Ordering::SeqCst);
        Ok(crate::registry::AskDisposition::ImmediateBytes(
            bytes::Bytes::from_owner(QaHiddenOwner {
                data: vec![7; 1024 * 1024],
                live: self.live.clone(),
            }),
        ))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn qa_scan_inline_budget_bounds_backing_owners() {
    let addr = "127.0.0.1:39922".parse().unwrap();
    let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        addr,
        crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing("qa-hidden-owner")),
            ..Default::default()
        },
    ));
    let live = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    registry
        .set_actor_ask_immediate_handler_sync(Arc::new(QaHiddenOwnerHandler {
            live: live.clone(),
            calls: calls.clone(),
        }))
        .await;
    let ctx = response_budget_read_context(&registry, addr);
    let (stream, mut peer) = tokio::io::duplex(4096);
    let (writer, task, _) = LockFreeStreamHandle::new(
        stream,
        addr,
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        Some(ctx),
    );
    write_empty_asks(&mut peer, 64).await;
    let handled = wait_for_stable_dispatch(&calls, Duration::from_secs(3)).await;
    let retained = live.load(Ordering::SeqCst);
    eprintln!(
        "inline hidden owners: handled={handled}/64 retained={retained} wire_payload_per_reply=1024"
    );
    writer.shutdown();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        live.load(Ordering::SeqCst),
        0,
        "shutdown must reclaim owners"
    );
    assert!(
        retained <= QA_RETAINED_BUDGET,
        "inline retained-byte budget counts slices, not actual backing owners: {retained}"
    );
}
