use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use bytes::Bytes;
use futures::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc;

use crate::{AskResponder, GossipError, RemoteConnection, Result};

struct ForwardTask {
    destination: RemoteConnection,
    actor_id: u64,
    type_hash: u32,
    payload: Bytes,
    responder: AskResponder,
    timeout: Option<Duration>,
    use_combined_timeout: bool,
    timeout_reply: Option<Bytes>,
    error_reply: Option<Bytes>,
}

struct AskForwarderInner {
    workers: Vec<mpsc::Sender<ForwardTask>>,
    next_worker: AtomicUsize,
}

const MAX_INFLIGHT_PER_WORKER: usize = 16;

#[derive(Clone)]
pub struct AskForwarder {
    inner: Arc<AskForwarderInner>,
}

pub trait AskForwardObserver: Send + Sync {
    fn record_success(&self);
    fn record_error(&self);
}

impl AskForwarder {
    pub fn new(workers: usize, capacity: usize) -> Self {
        Self::new_with_observer(workers, capacity, None)
    }

    pub fn new_with_observer(
        workers: usize,
        capacity: usize,
        completion_observer: Option<Arc<dyn AskForwardObserver>>,
    ) -> Self {
        let workers = workers.max(1);
        let capacity = capacity.max(128);
        let max_inflight = capacity.clamp(1, MAX_INFLIGHT_PER_WORKER);

        let mut worker_senders = Vec::with_capacity(workers);
        for _ in 0..workers {
            let (tx, mut rx) = mpsc::channel::<ForwardTask>(capacity);
            let worker_observer = completion_observer.clone();
            let handle = tokio::spawn(async move {
                let mut inflight = FuturesUnordered::new();
                let mut rx_closed = false;

                loop {
                    while inflight.len() < max_inflight {
                        match rx.try_recv() {
                            Ok(task) => inflight.push(Box::pin(run_forward_task_isolated(task))),
                            Err(mpsc::error::TryRecvError::Empty) => break,
                            Err(mpsc::error::TryRecvError::Disconnected) => {
                                rx_closed = true;
                                break;
                            }
                        }
                    }

                    if rx_closed && inflight.is_empty() {
                        break;
                    }

                    tokio::select! {
                        maybe_task = rx.recv(), if can_receive_more(rx_closed, inflight.len(), max_inflight) => {
                            match maybe_task {
                                Some(task) => inflight.push(Box::pin(run_forward_task_isolated(task))),
                                None => rx_closed = true,
                            }
                        }
                        Some(completed) = inflight.next(), if !inflight.is_empty() => {
                            if let Some(completed) = completed {
                                handle_completed_forward(completed, worker_observer.as_deref());
                            }
                        }
                    }
                }
            });
            worker_senders.push(tx);
            std::mem::drop(handle);
        }

        let inner = Arc::new(AskForwarderInner {
            workers: worker_senders,
            next_worker: AtomicUsize::new(0),
        });

        Self { inner }
    }

    pub fn try_forward_actor_ask_no_timeout(
        &self,
        destination: RemoteConnection,
        actor_id: u64,
        type_hash: u32,
        payload: Bytes,
        responder: AskResponder,
    ) -> Result<()> {
        self.try_send_task(ForwardTask {
            destination,
            actor_id,
            type_hash,
            payload,
            responder,
            timeout: None,
            use_combined_timeout: false,
            timeout_reply: None,
            error_reply: None,
        })
    }

    pub fn try_forward_actor_ask_with_timeout(
        &self,
        destination: RemoteConnection,
        actor_id: u64,
        type_hash: u32,
        payload: Bytes,
        timeout: Duration,
        responder: AskResponder,
        timeout_reply: Bytes,
        error_reply: Bytes,
    ) -> Result<()> {
        self.try_send_task(ForwardTask {
            destination,
            actor_id,
            type_hash,
            payload,
            responder,
            timeout: Some(timeout),
            use_combined_timeout: false,
            timeout_reply: Some(timeout_reply),
            error_reply: Some(error_reply),
        })
    }

    pub fn try_forward_actor_ask_combined_timeout(
        &self,
        destination: RemoteConnection,
        actor_id: u64,
        type_hash: u32,
        payload: Bytes,
        timeout: Duration,
        responder: AskResponder,
        timeout_reply: Bytes,
        error_reply: Bytes,
    ) -> Result<()> {
        self.try_send_task(ForwardTask {
            destination,
            actor_id,
            type_hash,
            payload,
            responder,
            timeout: Some(timeout),
            use_combined_timeout: true,
            timeout_reply: Some(timeout_reply),
            error_reply: Some(error_reply),
        })
    }

    fn try_send_task(&self, task: ForwardTask) -> Result<()> {
        let worker_count = self.inner.workers.len();
        let worker_idx = self.inner.next_worker.fetch_add(1, Ordering::Relaxed) % worker_count;
        self.inner.workers[worker_idx]
            .try_send(task)
            .map_err(|err| match err {
                mpsc::error::TrySendError::Full(_) => GossipError::WriteQueueFull,
                mpsc::error::TrySendError::Closed(_) => GossipError::Shutdown,
            })?;
        Ok(())
    }
}

enum ForwardOutcome {
    Success,
    Timeout,
    Error,
}

fn can_receive_more(rx_closed: bool, inflight_len: usize, max_inflight: usize) -> bool {
    !rx_closed && inflight_len < max_inflight
}

/// ACTOR_REM_2 R16k: isolate a panicking forwarded-ask future so it kills only
/// that one forward, not the shared worker task. Without this, a panic anywhere
/// in the awaited forward chain unwinds the whole worker; every subsequent send
/// to that worker's channel then maps to `GossipError::Shutdown`, permanently
/// losing `1/workers` of forwarding capacity per panic. Returns `None` when the
/// forward panicked (its responder is dropped, so the caller fails/times out as
/// it would for any transport error).
async fn run_forward_task_isolated(task: ForwardTask) -> Option<ForwardOutcome> {
    std::panic::AssertUnwindSafe(run_forward_task(task))
        .catch_unwind()
        .await
        .ok()
}

/// Runs the forward and delivers its reply before resolving. Delivery is
/// awaited here, inside the same future the worker's `inflight` set tracks,
/// rather than handed to a detached task — a claimed reply guard is only
/// ever released once delivery has actually completed (sent inline or
/// retried to completion), never left pending on an untracked task that
/// could go unpolled if the forward's own future were dropped.
async fn run_forward_task(task: ForwardTask) -> ForwardOutcome {
    let response = match task.timeout {
        Some(timeout) if task.use_combined_timeout => {
            task.destination
                .ask_actor_frame(task.actor_id, task.type_hash, task.payload, timeout)
                .await
        }
        Some(timeout) => tokio::time::timeout(
            timeout,
            task.destination.ask_actor_frame_no_timeout(
                task.actor_id,
                task.type_hash,
                task.payload,
            ),
        )
        .await
        .map_err(|_| GossipError::Timeout)
        .and_then(|reply| reply),
        None => {
            task.destination
                .ask_actor_frame_no_timeout(task.actor_id, task.type_hash, task.payload)
                .await
        }
    };

    match response {
        Ok(reply) => {
            deliver_forwarded_reply(task.responder, reply).await;
            ForwardOutcome::Success
        }
        Err(GossipError::Timeout) => {
            if let Some(reply) = task.timeout_reply {
                deliver_forwarded_reply(task.responder, reply).await;
            }
            ForwardOutcome::Timeout
        }
        Err(_) => {
            if let Some(reply) = task.error_reply {
                deliver_forwarded_reply(task.responder, reply).await;
            }
            ForwardOutcome::Error
        }
    }
}

fn handle_completed_forward(
    outcome: ForwardOutcome,
    completion_observer: Option<&dyn AskForwardObserver>,
) {
    let Some(observer) = completion_observer else {
        return;
    };
    match outcome {
        ForwardOutcome::Success => observer.record_success(),
        ForwardOutcome::Timeout | ForwardOutcome::Error => observer.record_error(),
    }
}

/// Deliver an already-computed forwarded reply, awaiting to completion so
/// this call's claim on the ask's single-use reply guard is never released
/// (by this future resolving) without the reply having actually been sent or
/// handed to a completed, guaranteed retry. See
/// [`AskResponder::reply_bytes_guaranteed`] for the delivery/claim contract.
async fn deliver_forwarded_reply(responder: AskResponder, reply: Bytes) {
    if let Err(err) = responder.reply_bytes_guaranteed(reply).await {
        if !is_duplicate_reply_claim(&err) {
            tracing::warn!(
                error = %err,
                "forwarded ask reply delivery failed"
            );
        }
    }
}

/// True when `err` is the single-use guard's duplicate-claim rejection
/// (see `ask_responder::claim_reply`) rather than a genuine delivery
/// failure worth logging.
fn is_duplicate_reply_claim(err: &GossipError) -> bool {
    matches!(err, GossipError::Network(e) if e.kind() == std::io::ErrorKind::AlreadyExists)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receive_guard_disables_reads_at_inflight_limit() {
        assert!(can_receive_more(
            false,
            MAX_INFLIGHT_PER_WORKER - 1,
            MAX_INFLIGHT_PER_WORKER
        ));
        assert!(!can_receive_more(
            false,
            MAX_INFLIGHT_PER_WORKER,
            MAX_INFLIGHT_PER_WORKER
        ));
        assert!(!can_receive_more(
            false,
            MAX_INFLIGHT_PER_WORKER + 1,
            MAX_INFLIGHT_PER_WORKER
        ));
    }

    #[test]
    fn receive_guard_disables_reads_after_channel_closes() {
        assert!(!can_receive_more(true, 0, MAX_INFLIGHT_PER_WORKER));
    }

    use crate::connection_pool::{BufferConfig, ChannelId, LockFreeStreamHandle};
    use std::sync::atomic::AtomicBool;
    use tokio::io::AsyncReadExt;

    fn test_addr() -> std::net::SocketAddr {
        "127.0.0.1:28888".parse().expect("valid test addr")
    }

    /// Read everything available from `peer` without depending on the
    /// connection being shut down first: genuine EOF/read-error stops the
    /// read, but so does a run of `quiet_rounds` consecutive per-read
    /// timeouts, treated as "the writer has nothing left to flush". A bare
    /// timeout on its own is tolerated (not treated as end of stream) since
    /// the writer task may need a few scheduling rounds to drain its backlog
    /// before producing more bytes; each `.await` here is itself what gives
    /// the writer task (and any concurrently spawned delivery) a chance to
    /// run. Bounded by `max_iterations` so a genuinely stuck test still fails
    /// fast instead of hanging.
    async fn read_all_available(
        peer: &mut tokio::io::DuplexStream,
        max_iterations: u32,
        quiet_rounds: u32,
    ) -> Vec<u8> {
        let mut buf = vec![0u8; 4096];
        let mut all = Vec::new();
        let mut consecutive_timeouts = 0u32;
        for _ in 0..max_iterations {
            match tokio::time::timeout(Duration::from_millis(100), peer.read(&mut buf)).await {
                Ok(Ok(0)) => break,
                Ok(Ok(n)) => {
                    all.extend_from_slice(&buf[..n]);
                    consecutive_timeouts = 0;
                }
                Ok(Err(_)) => break,
                Err(_) => {
                    consecutive_timeouts += 1;
                    if consecutive_timeouts >= quiet_rounds {
                        break;
                    }
                }
            }
        }
        all
    }

    /// A forwarded reply that arrives while the connection's ordinary write
    /// queue is already saturated must still reach the peer — never be
    /// silently dropped just because the nonblocking fast path rejected it
    /// once. `deliver_forwarded_reply` only returns once delivery has
    /// actually completed, so awaiting it here (concurrently with draining
    /// the backlog) is itself the proof, with no arbitrary sleep needed.
    #[tokio::test]
    async fn forwarded_reply_survives_full_normal_write_queue() {
        let (client, mut peer) = tokio::io::duplex(64 * 1024);
        let buffer_config = BufferConfig::default().with_write_queue_capacity(128);
        let (stream_handle, task, _) =
            LockFreeStreamHandle::new(client, test_addr(), ChannelId::TellAsk, buffer_config, None, None);
        let stream_handle = Arc::new(stream_handle);

        // Saturate the ordinary (non-immediate) write queue synchronously, so
        // the background writer task never gets a chance to drain it before
        // the forwarded reply below is delivered.
        let mut filled = 0u32;
        loop {
            let used = Arc::new(AtomicBool::new(false));
            let filler =
                AskResponder::from_stream_handle(1_000 + filled, stream_handle.clone(), used);
            match filler.try_reply_bytes(Bytes::from_static(b"filler")) {
                Ok(()) => filled += 1,
                Err(_) => break,
            }
            assert!(filled < 4096, "normal write queue never saturated");
        }
        assert!(
            filled >= 128,
            "expected the normal write queue to saturate at its configured \
             128-slot capacity; only {filled} filler frames were admitted"
        );

        let used = Arc::new(AtomicBool::new(false));
        let responder = AskResponder::from_stream_handle(42, stream_handle.clone(), used);

        // Drain the peer concurrently so the writer task can flush the filler
        // backlog, freeing capacity for a retried reply.
        let drain = tokio::spawn(async move { read_all_available(&mut peer, 200, 5).await });

        // This is the exact call the worker's forward future makes once the
        // remote call resolves. At the time of this call the queue above is
        // already full; the call does not return until the retry (once the
        // drain above frees capacity) has actually completed.
        deliver_forwarded_reply(responder, Bytes::from_static(b"forwarded-reply-payload")).await;

        let written = drain.await.expect("drain task must not panic");
        stream_handle.shutdown();
        assert!(
            written
                .windows(b"forwarded-reply-payload".len())
                .any(|w| w == b"forwarded-reply-payload"),
            "the forwarded reply must be delivered, not silently dropped, when \
             the normal write queue was full at completion time"
        );

        let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
    }

    /// A forwarded reply whose responder shares its single-use claim with a
    /// sibling that already sent a reply for the same ask must never be
    /// retried — retrying would put a second, duplicate Response frame on the
    /// wire for one correlation id.
    #[tokio::test]
    async fn sibling_reply_after_claim_is_consumed_is_dropped_not_retried() {
        let (client, mut peer) = tokio::io::duplex(64 * 1024);
        let buffer_config = BufferConfig::default().with_write_queue_capacity(128);
        let (stream_handle, task, _) =
            LockFreeStreamHandle::new(client, test_addr(), ChannelId::TellAsk, buffer_config, None, None);
        let stream_handle = Arc::new(stream_handle);

        // Two sibling responders sharing one guard, as minted from one AskContext.
        let used = Arc::new(AtomicBool::new(false));
        let first = AskResponder::from_stream_handle(7, stream_handle.clone(), used.clone());
        let second = AskResponder::from_stream_handle(7, stream_handle.clone(), used.clone());

        deliver_forwarded_reply(first, Bytes::from_static(b"first-reply-payload")).await;
        deliver_forwarded_reply(second, Bytes::from_static(b"second-reply-payload")).await;

        let written = read_all_available(&mut peer, 200, 5).await;
        stream_handle.shutdown();

        assert!(
            written
                .windows(b"first-reply-payload".len())
                .any(|w| w == b"first-reply-payload"),
            "the first (owning) reply must still be delivered"
        );
        assert!(
            !written
                .windows(b"second-reply-payload".len())
                .any(|w| w == b"second-reply-payload"),
            "a sibling reply after the guard was already claimed must be \
             dropped, not retried, to avoid a duplicate response on the same \
             correlation id"
        );

        let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
    }

    /// The responder that wins the single-use claim has its OWN nonblocking
    /// enqueue rejected (full queue), and a second, sibling responder for the
    /// same ask arrives right after. `deliver_forwarded_reply` does not
    /// return for the winner until its
    /// retry has actually completed, so simply awaiting both calls in order
    /// (no sleep) is a deterministic proof that the winner's reply is still
    /// delivered and the sibling is still dropped, never both silently lost
    /// nor both sent.
    #[tokio::test]
    async fn claimed_reply_whose_own_enqueue_fails_is_still_delivered_not_dropped() {
        let (client, mut peer) = tokio::io::duplex(64 * 1024);
        let buffer_config = BufferConfig::default().with_write_queue_capacity(128);
        let (stream_handle, task, _) =
            LockFreeStreamHandle::new(client, test_addr(), ChannelId::TellAsk, buffer_config, None, None);
        let stream_handle = Arc::new(stream_handle);

        // Saturate the ordinary write queue synchronously so the winning
        // claimant's own nonblocking send below is rejected too.
        let mut filled = 0u32;
        loop {
            let used = Arc::new(AtomicBool::new(false));
            let filler =
                AskResponder::from_stream_handle(2_000 + filled, stream_handle.clone(), used);
            match filler.try_reply_bytes(Bytes::from_static(b"filler")) {
                Ok(()) => filled += 1,
                Err(_) => break,
            }
            assert!(filled < 4096, "normal write queue never saturated");
        }
        assert!(filled >= 128, "expected the normal write queue to saturate");

        // Two sibling responders sharing one guard, as minted from one AskContext.
        let used = Arc::new(AtomicBool::new(false));
        let winner = AskResponder::from_stream_handle(8, stream_handle.clone(), used.clone());
        let sibling = AskResponder::from_stream_handle(8, stream_handle.clone(), used.clone());

        // Drain the peer concurrently so the writer task can flush the filler
        // backlog, freeing capacity for the winner's retried reply.
        let drain = tokio::spawn(async move { read_all_available(&mut peer, 200, 5).await });

        // Winner claims the guard; its own nonblocking send is rejected
        // because the queue above is already full. This does not return
        // until the retry actually delivers (once the drain above frees
        // capacity) — proving the claim is never released undelivered.
        deliver_forwarded_reply(winner, Bytes::from_static(b"winner-reply-payload")).await;
        // Sibling arrives after the claim is gone and must be dropped.
        deliver_forwarded_reply(sibling, Bytes::from_static(b"sibling-reply-payload")).await;

        let written = drain.await.expect("drain task must not panic");
        stream_handle.shutdown();
        assert!(
            written
                .windows(b"winner-reply-payload".len())
                .any(|w| w == b"winner-reply-payload"),
            "the claimant's reply must still be delivered via retry even \
             though its own first enqueue attempt was rejected by the full \
             queue — the origin ask must not time out"
        );
        assert!(
            !written
                .windows(b"sibling-reply-payload".len())
                .any(|w| w == b"sibling-reply-payload"),
            "the sibling must never resend once the guard is claimed, \
             regardless of whether the claimant's own send succeeded inline \
             or needed a retry"
        );

        let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
    }
}
