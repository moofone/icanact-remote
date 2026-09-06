const F02_CHILD_ENV: &str = "ICANACT_QA_F02_CHILD";

/// Parent owns the watchdog. The child runs the current-thread executor that
/// used to freeze inside teardown; a Tokio timeout on that executor cannot
/// prove liveness if it is stuck.
#[test]
fn qa_f02_full_queue_close_keeps_executor_live() {
    if std::env::var(F02_CHILD_ENV).ok().as_deref() == Some("1") {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime");
        rt.block_on(f02_child_body());
        return;
    }

    let current_exe = std::env::current_exe().expect("current test executable");
    let mut child = std::process::Command::new(current_exe)
        .arg("--exact")
        .arg("connection_pool::tests::qa_f02_full_queue_close_keeps_executor_live")
        .arg("--nocapture")
        .env(F02_CHILD_ENV, "1")
        .spawn()
        .expect("spawn F02 child");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                assert!(
                    status.success(),
                    "F02 child must keep the executor live, got {status}"
                );
                return;
            }
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "F02 child did not finish within 5s after writer abort with a parked full-queue sender"
                );
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(20)),
            Err(err) => panic!("failed waiting for F02 child: {err}"),
        }
    }
}

async fn f02_child_body() {
    parked_sender_finishes_after_abort().await;
    concurrent_enqueue_unblocks_on_close().await;
}

async fn parked_sender_finishes_after_abort() {
    let (stream, _peer) = tokio::io::duplex(1);
    let (writer, task, _) = LockFreeStreamHandle::new(
        stream,
        "127.0.0.1:29881".parse().unwrap(),
        ChannelId::TellAsk,
        BufferConfig::default().with_write_queue_capacity(128),
        None,
        None,
    );
    tokio::task::yield_now().await;
    let writer = Arc::new(writer);
    let header =
        crate::framing::try_write_ask_response_header(crate::MessageType::Response, 1, 1).unwrap();
    let mut accepted = 0;
    while writer
        .write_header_and_payload_control_inline_nonblocking(
            header,
            16,
            bytes::Bytes::from_static(b"a"),
        )
        .is_ok()
    {
        accepted += 1;
    }
    assert_eq!(accepted, 128);
    let parked = Arc::new(AtomicBool::new(false));
    let sender_writer = writer.clone();
    let sender_parked = parked.clone();
    let future = async move {
        let send = sender_writer.write_header_and_payload_control_inline(
            header,
            16,
            bytes::Bytes::from_static(b"b"),
        );
        tokio::pin!(send);
        std::future::poll_fn(|cx| {
            let result = send.as_mut().poll(cx);
            if result.is_pending() {
                sender_parked.store(true, Ordering::SeqCst);
            }
            result
        })
        .await
    };
    let mut future = Box::pin(future);
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    assert!(parked.load(Ordering::SeqCst));
    let sender = tokio::spawn(future);
    task.abort();
    let _ = task.await;
    let result = tokio::time::timeout(Duration::from_secs(1), sender).await;
    assert!(
        result.is_ok(),
        "parked sender must finish when writer exits"
    );
}

async fn concurrent_enqueue_unblocks_on_close() {
    let (stream, _peer) = tokio::io::duplex(1);
    let (writer, task, _) = LockFreeStreamHandle::new(
        stream,
        "127.0.0.1:29882".parse().unwrap(),
        ChannelId::TellAsk,
        BufferConfig::default().with_write_queue_capacity(32),
        None,
        None,
    );
    tokio::task::yield_now().await;
    let writer = Arc::new(writer);
    let header =
        crate::framing::try_write_ask_response_header(crate::MessageType::Response, 1, 1).unwrap();
    while writer
        .write_header_and_payload_control_inline_nonblocking(
            header,
            16,
            bytes::Bytes::from_static(b"a"),
        )
        .is_ok()
    {}
    let mut parked = Vec::new();
    for _ in 0..4 {
        let sender_writer = writer.clone();
        parked.push(tokio::spawn(async move {
            sender_writer
                .write_header_and_payload_control_inline(
                    header,
                    16,
                    bytes::Bytes::from_static(b"c"),
                )
                .await
        }));
    }
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    writer.shutdown();
    task.abort();
    let _ = task.await;
    for (i, handle) in parked.into_iter().enumerate() {
        let _result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap_or_else(|_| panic!("concurrent producer {i} must unblock on close"))
            .expect("concurrent producer task panicked");
    }
}
