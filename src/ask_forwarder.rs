use std::collections::VecDeque;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::{AbortHandle, JoinHandle};

use crate::ask_responder::TryReplyError;
use crate::{AskResponder, GossipError, RemoteConnection, Result};

struct ForwardTask {
    destination: RemoteConnection,
    actor_id: u64,
    type_hash: u32,
    payload: Bytes,
    responder: AskResponder,
    deadline: Option<Instant>,
    timeout_reply: Option<Bytes>,
    error_reply: Option<Bytes>,
    _permit: Option<OwnedSemaphorePermit>,
}

struct WorkerControl {
    closed: AtomicBool,
    remaining: AtomicUsize,
    done: Notify,
}

struct AskForwarderInner {
    workers: Vec<mpsc::Sender<ForwardTask>>,
    next_worker: AtomicUsize,
    control: Arc<WorkerControl>,
    abort_handles: Vec<AbortHandle>,
    joins: Mutex<Vec<JoinHandle<()>>>,
    permits: Arc<Semaphore>,
}

const MAX_INFLIGHT_PER_WORKER: usize = 16;

/// Bounded local forwarding of actor asks onto a shared destination connection.
///
/// Timed methods establish one absolute deadline at admission. That budget
/// covers worker-queue wait, destination admission/response, and a single
/// nonblocking terminal-reply attempt after expiry. Enqueue success is not
/// remote receipt. No-timeout forwards stay deadline-free but are cancelled
/// when the last owner is dropped or [`AskForwarder::shutdown`] expires.
///
/// Final-owner drop aborts outstanding work without waiting; reclamation is
/// executor-driven. Call [`AskForwarder::shutdown`] when the caller needs
/// completion evidence. Dropping one clone leaves other live owners usable.
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
        let permit_limit = capacity.saturating_add(MAX_INFLIGHT_PER_WORKER);
        let permits = Arc::new(Semaphore::new(permit_limit));
        let control = Arc::new(WorkerControl {
            closed: AtomicBool::new(false),
            remaining: AtomicUsize::new(workers),
            done: Notify::new(),
        });

        let mut worker_senders = Vec::with_capacity(workers);
        let mut abort_handles = Vec::with_capacity(workers);
        let mut joins = Vec::with_capacity(workers);
        for _ in 0..workers {
            let (tx, rx) = mpsc::channel::<ForwardTask>(capacity);
            let worker_observer = completion_observer.clone();
            let worker_control = control.clone();
            let handle = tokio::spawn(run_forward_worker(
                rx,
                worker_observer,
                worker_control,
                max_inflight,
            ));
            abort_handles.push(handle.abort_handle());
            joins.push(handle);
            worker_senders.push(tx);
        }

        let inner = Arc::new(AskForwarderInner {
            workers: worker_senders,
            next_worker: AtomicUsize::new(0),
            control,
            abort_handles,
            joins: Mutex::new(joins),
            permits,
        });

        Self { inner }
    }

    /// Close admission, drain until `grace`, then abort remaining work and
    /// wait for workers to finish. Repeated calls observe the same terminal
    /// state. A timeout is returned as [`GossipError::Timeout`] rather than
    /// success.
    pub async fn shutdown(&self, grace: Duration) -> Result<()> {
        self.inner.control.closed.store(true, Ordering::Release);
        if wait_for_workers(&self.inner.control, grace).await {
            return Ok(());
        }
        for handle in &self.inner.abort_handles {
            handle.abort();
        }
        let joins = {
            let mut guard = self
                .inner
                .joins
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            std::mem::take(&mut *guard)
        };
        for join in joins {
            let _ = join.await;
        }
        if wait_for_workers(&self.inner.control, Duration::from_secs(2)).await {
            Ok(())
        } else {
            Err(GossipError::Timeout)
        }
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
            deadline: None,
            timeout_reply: None,
            error_reply: None,
            _permit: None,
        })
    }

    /// Forward with a destination timeout. The duration is converted to an
    /// absolute deadline at this call; queue wait, destination wait, and
    /// reply handoff share that budget.
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
        let deadline = admission_deadline(timeout);
        self.try_send_task(ForwardTask {
            destination,
            actor_id,
            type_hash,
            payload,
            responder,
            deadline: Some(deadline),
            timeout_reply: Some(timeout_reply),
            error_reply: Some(error_reply),
            _permit: None,
        })
    }

    /// Forward with a combined destination+response timeout. Same admission
    /// deadline contract as [`Self::try_forward_actor_ask_with_timeout`].
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
        let deadline = admission_deadline(timeout);
        self.try_send_task(ForwardTask {
            destination,
            actor_id,
            type_hash,
            payload,
            responder,
            deadline: Some(deadline),
            timeout_reply: Some(timeout_reply),
            error_reply: Some(error_reply),
            _permit: None,
        })
    }

    fn try_send_task(&self, mut task: ForwardTask) -> Result<()> {
        if self.inner.control.closed.load(Ordering::Acquire) {
            return Err(GossipError::Shutdown);
        }
        let permit = self
            .inner
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| GossipError::WriteQueueFull)?;
        task._permit = Some(permit);
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

impl Drop for AskForwarderInner {
    fn drop(&mut self) {
        self.control.closed.store(true, Ordering::Release);
        for handle in &self.abort_handles {
            handle.abort();
        }
    }
}

enum ForwardOutcome {
    Success,
    Timeout,
    Error,
    ReplyUndeliverable,
}

fn admission_deadline(timeout: Duration) -> Instant {
    // A zero duration is already expired: enqueue it so the worker delivers
    // `timeout_reply` without sending a destination ask, matching the prior
    // queued-timeout contract.
    if timeout.is_zero() {
        Instant::now()
    } else {
        saturating_deadline(Instant::now(), timeout)
    }
}

fn saturating_deadline(started_at: Instant, timeout: Duration) -> Instant {
    if let Some(deadline) = started_at.checked_add(timeout) {
        return deadline;
    }
    let mut nanos = timeout.as_nanos();
    while nanos > 0 {
        let chunk = u64::try_from(nanos).unwrap_or(u64::MAX);
        if let Some(deadline) = started_at.checked_add(Duration::from_nanos(chunk)) {
            return deadline;
        }
        nanos /= 2;
    }
    started_at
}

fn remaining_until(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

async fn wait_for_workers(control: &WorkerControl, grace: Duration) -> bool {
    let sleep = tokio::time::sleep(grace);
    tokio::pin!(sleep);
    loop {
        if control.remaining.load(Ordering::Acquire) == 0 {
            return true;
        }
        let notified = control.done.notified();
        tokio::select! {
            _ = notified => {}
            _ = &mut sleep => {
                return control.remaining.load(Ordering::Acquire) == 0;
            }
        }
    }
}

struct WorkerExit {
    control: Arc<WorkerControl>,
}

impl Drop for WorkerExit {
    fn drop(&mut self) {
        if self.control.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.control.done.notify_waiters();
        }
    }
}

async fn run_forward_worker(
    mut rx: mpsc::Receiver<ForwardTask>,
    worker_observer: Option<Arc<dyn AskForwardObserver>>,
    control: Arc<WorkerControl>,
    max_inflight: usize,
) {
    let _exit = WorkerExit {
        control: control.clone(),
    };
    let mut inflight = FuturesUnordered::new();
    let mut waiting: VecDeque<ForwardTask> = VecDeque::new();
    let mut rx_closed = false;

    loop {
        if control.closed.load(Ordering::Acquire) && waiting.is_empty() && inflight.is_empty() {
            rx.close();
            rx_closed = true;
        }
        expire_waiting(&mut waiting, worker_observer.as_deref());
        dispatch_waiting(
            &mut waiting,
            &mut inflight,
            max_inflight,
            worker_observer.as_deref(),
        );

        if rx_closed && waiting.is_empty() && inflight.is_empty() {
            break;
        }

        let earliest = earliest_deadline(&waiting);
        tokio::select! {
            maybe_task = rx.recv() => {
                match maybe_task {
                    Some(task) => waiting.push_back(task),
                    None => rx_closed = true,
                }
            }
            Some(completed) = inflight.next(), if !inflight.is_empty() => {
                if let Some(completed) = completed {
                    handle_completed_forward(completed, worker_observer.as_deref());
                }
            }
            _ = sleep_until_deadline(earliest), if earliest.is_some() => {}
        }
    }
}

fn earliest_deadline(waiting: &VecDeque<ForwardTask>) -> Option<Instant> {
    waiting.iter().filter_map(|task| task.deadline).min()
}

async fn sleep_until_deadline(deadline: Option<Instant>) {
    let Some(deadline) = deadline else {
        std::future::pending::<()>().await;
        return;
    };
    let wait = remaining_until(deadline);
    if wait.is_zero() {
        return;
    }
    tokio::time::sleep(wait).await;
}

fn expire_waiting(waiting: &mut VecDeque<ForwardTask>, observer: Option<&dyn AskForwardObserver>) {
    let now = Instant::now();
    let mut index = 0usize;
    while index < waiting.len() {
        if waiting[index]
            .deadline
            .is_some_and(|deadline| deadline <= now)
        {
            let task = waiting.remove(index).expect("index in range");
            complete_expired_queued(task, observer);
        } else {
            index += 1;
        }
    }
}

fn dispatch_waiting(
    waiting: &mut VecDeque<ForwardTask>,
    inflight: &mut FuturesUnordered<
        std::pin::Pin<Box<dyn std::future::Future<Output = Option<ForwardOutcome>> + Send>>,
    >,
    max_inflight: usize,
    observer: Option<&dyn AskForwardObserver>,
) {
    while inflight.len() < max_inflight {
        let Some(task) = waiting.pop_front() else {
            break;
        };
        if task
            .deadline
            .is_some_and(|deadline| deadline <= Instant::now())
        {
            complete_expired_queued(task, observer);
            continue;
        }
        inflight.push(Box::pin(run_forward_task_isolated(task)));
    }
}

fn complete_expired_queued(task: ForwardTask, observer: Option<&dyn AskForwardObserver>) {
    let outcome = if let Some(reply) = task.timeout_reply.clone() {
        if try_deliver_terminal_reply(task.responder, reply) {
            ForwardOutcome::Timeout
        } else {
            ForwardOutcome::ReplyUndeliverable
        }
    } else {
        drop(task.responder);
        ForwardOutcome::Timeout
    };
    handle_completed_forward(outcome, observer);
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
/// awaited here, inside the same future the worker's `inflight` set tracks.
/// Timed work uses the admission deadline; expiry never restarts the original
/// duration and reply handoff after the deadline is a single nonblocking try.
async fn run_forward_task(task: ForwardTask) -> ForwardOutcome {
    if let Some(deadline) = task.deadline
        && remaining_until(deadline).is_zero()
    {
        return terminal_timeout(task);
    }

    let remaining = task.deadline.map(remaining_until);
    let destination = task.destination.clone();
    let actor_id = task.actor_id;
    let type_hash = task.type_hash;
    let payload = task.payload.clone();
    // Timed forwards always use `ask_actor_frame` so one SlotGuard covers
    // identify-gate wait, write-queue admission, and the response wait.
    // Timeout/cancel drops that guard and unregisters the correlation before
    // returning `GossipError::Timeout`.
    let response = match remaining {
        Some(timeout) => {
            destination
                .ask_actor_frame(actor_id, type_hash, payload, timeout)
                .await
        }
        None => {
            destination
                .ask_actor_frame_no_timeout(actor_id, type_hash, payload)
                .await
        }
    };

    match response {
        Ok(reply) => deliver_result_reply(task, reply, ForwardOutcome::Success).await,
        Err(GossipError::Timeout) => terminal_timeout(task),
        Err(_) => {
            if let Some(reply) = task.error_reply.clone() {
                deliver_result_reply(task, reply, ForwardOutcome::Error).await
            } else {
                ForwardOutcome::Error
            }
        }
    }
}

fn terminal_timeout(task: ForwardTask) -> ForwardOutcome {
    if let Some(reply) = task.timeout_reply {
        if try_deliver_terminal_reply(task.responder, reply) {
            ForwardOutcome::Timeout
        } else {
            ForwardOutcome::ReplyUndeliverable
        }
    } else {
        ForwardOutcome::Timeout
    }
}

async fn deliver_result_reply(
    task: ForwardTask,
    reply: Bytes,
    success: ForwardOutcome,
) -> ForwardOutcome {
    match task.deadline {
        None => {
            deliver_forwarded_reply(task.responder, reply).await;
            success
        }
        Some(deadline) => {
            let remaining = remaining_until(deadline);
            if remaining.is_zero() {
                if try_deliver_terminal_reply(task.responder, reply) {
                    success
                } else {
                    ForwardOutcome::ReplyUndeliverable
                }
            } else {
                match tokio::time::timeout(
                    remaining,
                    deliver_forwarded_reply_outcome(task.responder, reply),
                )
                .await
                {
                    Ok(true) => success,
                    Ok(false) => ForwardOutcome::ReplyUndeliverable,
                    Err(_) => ForwardOutcome::ReplyUndeliverable,
                }
            }
        }
    }
}

async fn deliver_forwarded_reply_outcome(responder: AskResponder, reply: Bytes) -> bool {
    match responder.reply_bytes_guaranteed(reply).await {
        Ok(()) => true,
        Err(err) if is_duplicate_reply_claim(&err) => true,
        Err(err) => {
            tracing::warn!(error = %err, "forwarded ask reply delivery failed");
            false
        }
    }
}

fn try_deliver_terminal_reply(responder: AskResponder, reply: Bytes) -> bool {
    match responder.try_reply_bytes_with_fallback(reply) {
        Ok(()) => true,
        Err(TryReplyError::ClaimUnavailable(_)) => false,
        Err(TryReplyError::Enqueue(_)) => false,
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
        ForwardOutcome::Timeout | ForwardOutcome::Error | ForwardOutcome::ReplyUndeliverable => {
            observer.record_error()
        }
    }
}

/// Deliver an already-computed forwarded reply, awaiting to completion so
/// this call's claim on the ask's single-use reply guard is never released
/// (by this future resolving) without the reply having actually been sent or
/// handed to a completed, guaranteed retry. See
/// [`AskResponder::reply_bytes_guaranteed`] for the delivery/claim contract.
async fn deliver_forwarded_reply(responder: AskResponder, reply: Bytes) {
    let _ = deliver_forwarded_reply_outcome(responder, reply).await;
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
        let (stream_handle, task, _) = LockFreeStreamHandle::new(
            client,
            test_addr(),
            ChannelId::TellAsk,
            buffer_config,
            None,
            None,
        );
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
        let (stream_handle, task, _) = LockFreeStreamHandle::new(
            client,
            test_addr(),
            ChannelId::TellAsk,
            buffer_config,
            None,
            None,
        );
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
        let (stream_handle, task, _) = LockFreeStreamHandle::new(
            client,
            test_addr(),
            ChannelId::TellAsk,
            buffer_config,
            None,
            None,
        );
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

    #[test]
    fn zero_duration_is_an_already_expired_deadline() {
        let deadline = admission_deadline(Duration::ZERO);
        assert!(deadline <= Instant::now());
    }
}
