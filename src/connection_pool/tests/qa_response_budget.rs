use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

// A lawful 64-outcome burst must still exceed the unchanged byte budget.
const QA_RESPONSE_OWNER_BYTES: usize = 512 * 1024;
const QA_ASK_COUNT: u32 = 512;
const QA_OUTCOME_WINDOW: u32 = outcome_window::OUTCOME_CAP as u32;
const QA_RETAINED_BUDGET: usize = 20 * 1024 * 1024;

struct OwnedResponse {
    data: Vec<u8>,
    live: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for OwnedResponse {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl Drop for OwnedResponse {
    fn drop(&mut self) {
        self.live.fetch_sub(self.data.len(), Ordering::SeqCst);
    }
}

struct ImmediateOwnedHandler {
    live: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
    payload_len: usize,
}

impl crate::registry::ActorAskImmediateHandlerSync for ImmediateOwnedHandler {
    fn handle_actor_ask_sync_immediate(
        &self,
        _: u64,
        _: u32,
        _: crate::AlignedBytes,
    ) -> crate::Result<crate::registry::AskDisposition> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.live.fetch_add(self.payload_len, Ordering::SeqCst);
        Ok(crate::registry::AskDisposition::ImmediateBytes(
            crate::ForwardPayload::from_test_owner(OwnedResponse {
                data: vec![7; self.payload_len],
                live: self.live.clone(),
            }, self.payload_len),
        ))
    }
}

fn response_budget_read_context(
    registry: &Arc<crate::registry::GossipRegistry<()>>,
    addr: std::net::SocketAddr,
) -> ReadContext {
    ReadContext {
        authenticated_peer_id: None,
        streaming_state_handoff: None,
        registry_weak: Arc::downgrade(registry),
        peer_addr: addr,
        session_source: addr,
        peer_id: None,
        max_message_size: 10 * 1024 * 1024,
        expected_schema_hash: None,
        aligned_pool: Arc::new(crate::AlignedBytesPool::default()),
        inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
        response_correlation: None,
        response_writer: None,
        tell_handler_sync: None,
        tell_handler_sync_context: None,
        ask_immediate_handler_sync: registry.actor_ask_immediate_handler_sync.load_full(),
        ask_handler_sync: None,
        sync_actor_handler: None,
    }
}

async fn wait_for_stable_dispatch(calls: &AtomicUsize, timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    let mut last = 0usize;
    let mut stable = 0u8;
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(15)).await;
        let now = calls.load(Ordering::SeqCst);
        if now > 0 && now == last {
            stable = stable.saturating_add(1);
            if stable >= 6 {
                return now;
            }
        } else {
            stable = 0;
        }
        last = now;
    }
    last
}

fn parse_response_header(
    header: &[u8; crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN],
) -> (usize, Option<crate::framing::AskNackReason>) {
    let control = crate::framing::decode_control(
        header[..crate::framing::LENGTH_PREFIX_LEN]
            .try_into()
            .unwrap(),
    )
    .expect("response control word");
    assert_eq!(control.kind, crate::framing::WireKind::Response);
    let payload_len = control
        .body_len
        .saturating_sub(crate::framing::ASK_RESPONSE_HEADER_LEN);
    let nack = crate::framing::ask_nack_reason(&header[crate::framing::LENGTH_PREFIX_LEN..]);
    (payload_len, nack)
}

async fn write_empty_asks<W>(peer: &mut W, count: u32)
where
    W: AsyncWriteExt + Unpin,
{
    for corr in 1..=count {
        peer.write_all(&crate::framing::try_write_actor_ask_header(corr, 1, 1, 0).unwrap())
            .await
            .unwrap();
    }
}

/// R1: a non-reading peer must not retain unbounded inline reply owners.
/// Exercise the unchanged byte plateau with protocol-lawful saturated
/// windows, and verify every physical teardown releases its native owners.
#[tokio::test(flavor = "current_thread")]
async fn qa_inline_response_retention_stays_bounded_when_peer_never_reads() {
    let addr = "127.0.0.1:39910".parse().unwrap();
    let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        addr,
        crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing("qa-response-budget")),
            ..Default::default()
        },
    ));
    let live = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    registry
        .set_actor_ask_immediate_handler_sync(Arc::new(ImmediateOwnedHandler {
            live: live.clone(),
            calls: calls.clone(),
            payload_len: QA_RESPONSE_OWNER_BYTES,
        }))
        .await;
    // Preserve 512 total non-reading requests across eight physical
    // sessions. Each session saturates one lawful 64-outcome window; no
    // peer reads replies. Teardown must restore the plateau before reuse.
    assert_eq!(QA_ASK_COUNT % QA_OUTCOME_WINDOW, 0);
    let mut submitted = 0;
    for _ in 0..QA_ASK_COUNT / QA_OUTCOME_WINDOW {
        let ctx = response_budget_read_context(&registry, addr);
        let (stream, mut peer) = tokio::io::duplex(64 * 1024);
        let (writer, task, _) = LockFreeStreamHandle::new(
            stream, addr, ChannelId::TellAsk, BufferConfig::default(), None, Some(ctx),
        );
        write_empty_asks(&mut peer, QA_OUTCOME_WINDOW).await;
        submitted += QA_OUTCOME_WINDOW;
        let handled = wait_for_stable_dispatch(&calls, Duration::from_secs(3)).await;
        assert!(handled > 0, "fixture must dispatch asks");
        let retained = live.load(Ordering::SeqCst);
        assert!(retained <= QA_RETAINED_BUDGET,
            "inline response byte cap bypassed: retained={retained}, dispatched={handled}");
        writer.shutdown();
        task.await.unwrap();
        assert_eq!(live.load(Ordering::SeqCst), 0, "cleanup must drop reply owners");
        drop(peer);
    }
    assert_eq!(submitted, QA_ASK_COUNT);
}

/// Every submitted ask must resolve as a response, a backpressure NACK, or
/// connection failure. Rejected work must not run the handler.
#[tokio::test(flavor = "current_thread")]
async fn qa_inline_response_overload_has_terminal_outcome_for_every_ask() {
    let addr = "127.0.0.1:39913".parse().unwrap();
    let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        addr,
        crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing("qa-response-outcomes")),
            ..Default::default()
        },
    ));
    let live = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    registry
        .set_actor_ask_immediate_handler_sync(Arc::new(ImmediateOwnedHandler {
            live: live.clone(),
            calls: calls.clone(),
            payload_len: QA_RESPONSE_OWNER_BYTES,
        }))
        .await;
    let ctx = response_budget_read_context(&registry, addr);
    let (stream, mut peer) = tokio::io::duplex(64 * 1024);
    let (writer, task, _) = LockFreeStreamHandle::new(
        stream,
        addr,
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        Some(ctx),
    );
    let mut responses = 0u32;
    let mut nacks = 0u32;
    let mut seen = vec![false; QA_ASK_COUNT as usize];
    let mut saturated_bursts = 0;
    let recovered = tokio::time::timeout(Duration::from_secs(15), async {
        let mut body = vec![0u8; QA_RESPONSE_OWNER_BYTES];
        // Stall each lawful 64-request burst before reading its terminal
        // outcomes. Drain those credits before submitting the next burst;
        // preserve 512 total requests and the original total recovery bound.
        for start in (1..=QA_ASK_COUNT).step_by(QA_OUTCOME_WINDOW as usize) {
            let calls_before = calls.load(Ordering::SeqCst);
            for corr in start..start + QA_OUTCOME_WINDOW {
                peer.write_all(&crate::framing::try_write_actor_ask_header(corr, 1, 1, 0).unwrap())
                    .await.unwrap();
            }
            let handled = wait_for_stable_dispatch(&calls, Duration::from_secs(3)).await;
            assert!(handled > calls_before, "fixture must dispatch asks");
            assert!(handled - calls_before < QA_OUTCOME_WINDOW as usize,
                "a non-reading saturated window must backpressure before dispatch");
            saturated_bursts += 1;
            let retained = live.load(Ordering::SeqCst);
            assert!(retained <= QA_RETAINED_BUDGET,
                "inline response byte cap bypassed: retained={retained}, dispatched={handled}");
            for _ in 0..QA_OUTCOME_WINDOW {
                let mut header = [0u8; crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN];
                peer.read_exact(&mut header).await.unwrap();
                let corr = u32::from_be_bytes(header[4..8].try_into().unwrap());
                assert!((start..start + QA_OUTCOME_WINDOW).contains(&corr));
                assert!(!std::mem::replace(&mut seen[corr as usize - 1], true),
                    "each request has exactly one terminal outcome");
                let (payload_len, nack) = parse_response_header(&header);
                if let Some(reason) = nack {
                    assert_eq!(reason, crate::framing::AskNackReason::Backpressure);
                    assert_eq!(payload_len, 0);
                    nacks += 1;
                } else {
                    assert_eq!(payload_len, QA_RESPONSE_OWNER_BYTES);
                    peer.read_exact(&mut body).await.unwrap();
                    assert!(body.iter().all(|&b| b == 7));
                    responses += 1;
                }
            }
        }
        assert!(seen.iter().all(|&present| present));
    }).await;
    assert!(
        recovered.is_ok(),
        "every submitted ask must produce a response or NACK, got responses={responses} nacks={nacks}"
    );
    assert_eq!(responses + nacks, QA_ASK_COUNT);
    assert_eq!(
        calls.load(Ordering::SeqCst) as u32,
        responses,
        "backpressure NACKs must be pre-dispatch"
    );
    assert_eq!(saturated_bursts, QA_ASK_COUNT / QA_OUTCOME_WINDOW,
        "every lawful burst must exercise pre-dispatch saturation");
    // Bounded deferral is a lawful admission outcome for a finite window;
    // the old 512-outstanding fixture could only force NACKs by violating
    // the protocol. Keep NACK accounting, exact terminals and dispatch counts.
    assert!(
        responses > 0,
        "some asks must still complete once the peer reads"
    );

    peer.write_all(&crate::framing::try_write_actor_ask_header(QA_ASK_COUNT + 1, 1, 1, 0).unwrap())
        .await
        .unwrap();
    let followup = tokio::time::timeout(Duration::from_secs(5), async {
        let mut header = [0u8; crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN];
        peer.read_exact(&mut header).await.unwrap();
        let (payload_len, nack) = parse_response_header(&header);
        assert!(
            nack.is_none(),
            "follow-up after drain must be a data response"
        );
        let mut body = vec![0u8; payload_len];
        peer.read_exact(&mut body).await.unwrap();
        assert!(body.iter().all(|&b| b == 7));
    })
    .await;
    assert!(
        followup.is_ok(),
        "later traffic must succeed after the burst drains"
    );

    writer.shutdown();
    task.await.unwrap();
    assert_eq!(
        live.load(Ordering::SeqCst),
        0,
        "cleanup must drop reply owners"
    );
}

/// Workload inside the budget still delivers every reply.
#[tokio::test(flavor = "current_thread")]
async fn qa_inline_response_healthy_burst_delivers_all() {
    let addr = "127.0.0.1:39914".parse().unwrap();
    let registry = Arc::new(crate::registry::GossipRegistry::<()>::new(
        addr,
        crate::GossipConfig {
            key_pair: Some(crate::KeyPair::new_for_testing("qa-response-healthy")),
            ..Default::default()
        },
    ));
    let live = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    registry
        .set_actor_ask_immediate_handler_sync(Arc::new(ImmediateOwnedHandler {
            live: live.clone(),
            calls: calls.clone(),
            payload_len: 64,
        }))
        .await;
    let ctx = response_budget_read_context(&registry, addr);
    let (stream, peer) = tokio::io::duplex(64 * 1024);
    let (writer, task, _) = LockFreeStreamHandle::new(
        stream,
        addr,
        ChannelId::TellAsk,
        BufferConfig::default(),
        None,
        Some(ctx),
    );
    let (mut peer_read, mut peer_write) = tokio::io::split(peer);
    let credits = Arc::new(tokio::sync::Semaphore::new(QA_OUTCOME_WINDOW as usize));
    let reply_credits = credits.clone();
    let reader = tokio::spawn(async move {
        let mut body = vec![0u8; 64];
        for _ in 1..=QA_ASK_COUNT {
            let mut header = [0u8; crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN];
            peer_read.read_exact(&mut header).await.unwrap();
            let (payload_len, nack) = parse_response_header(&header);
            assert!(nack.is_none(), "healthy burst must not NACK");
            assert_eq!(payload_len, 64);
            peer_read.read_exact(&mut body).await.unwrap();
            reply_credits.add_permits(1);
        }
    });
    for corr in 1..=QA_ASK_COUNT {
        credits.acquire().await.unwrap().forget();
        peer_write.write_all(&crate::framing::try_write_actor_ask_header(corr, 1, 1, 0).unwrap())
            .await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(10), reader)
        .await
        .expect("healthy burst must finish")
        .expect("healthy reader must not panic");
    assert_eq!(calls.load(Ordering::SeqCst) as u32, QA_ASK_COUNT);
    writer.shutdown();
    task.await.unwrap();
    assert_eq!(
        live.load(Ordering::SeqCst),
        0,
        "cleanup must drop reply owners"
    );
}
