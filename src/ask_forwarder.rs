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

#[derive(Clone, Copy)]
enum ShutdownResult {
    Drained,
    TimedOut,
}

impl ShutdownResult {
    fn into_result(self) -> Result<()> {
        match self {
            Self::Drained => Ok(()),
            Self::TimedOut => Err(GossipError::Timeout),
        }
    }
}

struct WorkerControl {
    closed: AtomicBool,
    remaining: AtomicUsize,
    done: Notify,
    // A worker can be parked in `rx.recv()` while admission is open. The
    // shutdown path uses this edge-triggered wake in addition to the atomic
    // state so closure is observed without waiting for grace to elapse.
    shutdown_wake: Notify,
    shutdown_in_progress: AtomicBool,
    shutdown_result: Mutex<Option<ShutdownResult>>,
    shutdown_done: Notify,
}

struct AskForwarderInner {
    // Serialize the closed check with the enqueue. This makes every task
    // admitted before closure visible to the draining worker and prevents an
    // `Ok(())` enqueue from racing a shutdown that closes the receiver.
    admission: Mutex<()>,
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
            shutdown_wake: Notify::new(),
            shutdown_in_progress: AtomicBool::new(false),
            shutdown_result: Mutex::new(None),
            shutdown_done: Notify::new(),
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
            admission: Mutex::new(()),
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
        let control = Arc::clone(&self.inner.control);
        loop {
            if let Some(result) = read_shutdown_result(&control) {
                return result.into_result();
            }
            if control
                .shutdown_in_progress
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
            let notified = control.shutdown_done.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = read_shutdown_result(&control) {
                return result.into_result();
            }
            notified.await;
        }

        let mut leader = ShutdownLeader {
            control: control.clone(),
            finished: false,
        };
        {
            // No successful enqueue can occur after this lock is released.
            // The worker can therefore close its receiver after one final
            // channel drain without dropping an accepted task.
            let _admission = self
                .inner
                .admission
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            control.closed.store(true, Ordering::Release);
        }
        // Wake every worker, including one already parked in `recv`. Repeated
        // notify_one calls also cover a worker that has not registered its
        // notified future yet.
        control.shutdown_wake.notify_waiters();
        for _ in &self.inner.workers {
            control.shutdown_wake.notify_one();
        }

        if wait_for_workers(&control, grace).await {
            complete_shutdown(&control, ShutdownResult::Drained);
            leader.finished = true;
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

        // Grace expiry is a real timeout even when abort/reclamation itself
        // succeeds. Report that fact consistently to every shutdown caller.
        complete_shutdown(&control, ShutdownResult::TimedOut);
        leader.finished = true;
        Err(GossipError::Timeout)
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
        let _admission = self
            .inner
            .admission
            .lock()
            .unwrap_or_else(|err| err.into_inner());
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

struct ShutdownLeader {
    control: Arc<WorkerControl>,
    finished: bool,
}

impl Drop for ShutdownLeader {
    fn drop(&mut self) {
        if !self.finished {
            self.control
                .shutdown_in_progress
                .store(false, Ordering::Release);
            self.control.shutdown_done.notify_waiters();
        }
    }
}

fn read_shutdown_result(control: &WorkerControl) -> Option<ShutdownResult> {
    control
        .shutdown_result
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .as_ref()
        .copied()
}

fn complete_shutdown(control: &WorkerControl, result: ShutdownResult) {
    let mut guard = control
        .shutdown_result
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    *guard = Some(result);
    control.shutdown_done.notify_waiters();
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
        let notified = control.done.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if control.remaining.load(Ordering::Acquire) == 0 {
            return true;
        }
        tokio::select! {
            _ = &mut notified => {}
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
        if control.closed.load(Ordering::Acquire) && !rx_closed {
            // Admission is serialized with shutdown, so this final try_recv
            // sweep sees every task whose sender returned Ok(()). Only after
            // the queue is empty is it safe to close the receiver.
            loop {
                match rx.try_recv() {
                    Ok(task) => waiting.push_back(task),
                    Err(mpsc::error::TryRecvError::Empty) => {
                        rx.close();
                        rx_closed = true;
                        break;
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        rx_closed = true;
                        break;
                    }
                }
            }
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
            _ = control.shutdown_wake.notified() => {}
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

    struct NeverResponds;

    impl crate::registry::ActorMessageHandler for NeverResponds {
        fn handle_actor_message(
            &self,
            _actor_id: u64,
            _type_hash: u32,
            _payload: crate::AlignedBytes,
            _correlation_id: Option<u32>,
        ) -> crate::registry::ActorMessageFuture<'_> {
            Box::pin(std::future::pending())
        }
    }

    async fn test_destination() -> (
        RemoteConnection,
        crate::GossipRegistryHandle,
        crate::GossipRegistryHandle,
    ) {
        let config = crate::GossipConfig {
            gossip_interval: Duration::from_secs(3_600),
            ..Default::default()
        };
        let source = crate::GossipRegistryHandle::new_with_transport_stack(
            "127.0.0.1:0".parse().unwrap(),
            crate::KeyPair::new_for_testing("ask-forwarder-shutdown-source").to_secret_key(),
            Some(config.clone()),
            crate::BuilderTlsBootstrap,
        )
        .await
        .expect("source registry");
        let sink = crate::GossipRegistryHandle::new_with_transport_stack(
            "127.0.0.1:0".parse().unwrap(),
            crate::KeyPair::new_for_testing("ask-forwarder-shutdown-sink").to_secret_key(),
            Some(config),
            crate::BuilderTlsBootstrap,
        )
        .await
        .expect("sink registry");
        sink.registry
            .set_actor_message_handler(Arc::new(NeverResponds))
            .await;
        source
            .add_peer(&sink.registry.peer_id)
            .await
            .connect(&sink.registry.bind_addr)
            .await
            .expect("connect source to sink");

        let destination = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Ok(peer) = source.lookup_peer(&sink.registry.peer_id).await
                    && let Some(connection) = peer.connection_ref()
                {
                    break connection;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("source must publish its destination connection");
        (destination, source, sink)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_drains_admitted_queue() {
        let (destination, source, sink) = test_destination().await;
        let addr = "127.0.0.1:0".parse().unwrap();
        let (io, mut peer) = tokio::io::duplex(65536);
        let (writer, task, _) = LockFreeStreamHandle::new(
            io,
            addr,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let writer = Arc::new(writer);
        let completed = Arc::new(AtomicUsize::new(0));
        struct CountCompletions(Arc<AtomicUsize>);
        impl AskForwardObserver for CountCompletions {
            fn record_success(&self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn record_error(&self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let forwarder = AskForwarder::new_with_observer(
            1,
            128,
            Some(Arc::new(CountCompletions(completed.clone()))),
        );
        let responder =
            AskResponder::from_stream_handle(10, writer.clone(), Arc::new(AtomicBool::new(false)));

        // The worker has not been polled yet, so this accepted task is still
        // in its channel when shutdown closes admission. Shutdown must receive
        // it, complete its expired terminal reply, and account for it.
        forwarder
            .try_forward_actor_ask_combined_timeout(
                destination,
                1,
                1,
                Bytes::from_static(b"request"),
                Duration::ZERO,
                responder,
                Bytes::from_static(b"drained-timeout-reply"),
                Bytes::from_static(b"error"),
            )
            .expect("the task must be admitted before shutdown");
        forwarder
            .shutdown(Duration::from_secs(1))
            .await
            .expect("shutdown should report a completed drain");
        assert_eq!(completed.load(Ordering::SeqCst), 1);

        let mut written = vec![0u8; 4096];
        let n = tokio::time::timeout(Duration::from_secs(2), peer.read(&mut written))
            .await
            .expect("drained terminal reply must reach the response writer")
            .expect("response writer read must succeed");
        assert!(
            written[..n]
                .windows(b"drained-timeout-reply".len())
                .any(|w| w == b"drained-timeout-reply"),
            "shutdown must not drop a task accepted before closure"
        );

        writer.shutdown();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        source.shutdown().await;
        sink.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_wakes_idle_worker() {
        let forwarder = AskForwarder::new(1, 128);
        // Establish that the worker is already parked in `recv` rather than
        // merely observing closure at its first poll.
        tokio::task::yield_now().await;
        let result = tokio::time::timeout(
            Duration::from_millis(200),
            forwarder.shutdown(Duration::from_secs(5)),
        )
        .await;
        if result.is_err() {
            // Keep a failed RED run from leaving the worker around until
            // runtime teardown; the assertion below still reports the wakeup.
            let _ = forwarder.shutdown(Duration::ZERO).await;
        }
        let shutdown = result.expect("shutdown must wake an idle worker");
        assert!(shutdown.is_ok(), "an idle worker should drain successfully");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_truthfully_reports_expiry() {
        let (destination, source, sink) = test_destination().await;
        let addr = "127.0.0.1:0".parse().unwrap();
        let (io, peer) = tokio::io::duplex(65536);
        let (writer, task, _) = LockFreeStreamHandle::new(
            io,
            addr,
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let writer = Arc::new(writer);
        let forwarder = AskForwarder::new(1, 128);
        forwarder
            .try_forward_actor_ask_no_timeout(
                destination.clone(),
                1,
                1,
                Bytes::from_static(b"request"),
                AskResponder::from_stream_handle(
                    11,
                    writer.clone(),
                    Arc::new(AtomicBool::new(false)),
                ),
            )
            .expect("the no-timeout task must be admitted");

        // Confirm that the task is in flight before expiring shutdown grace.
        tokio::time::timeout(Duration::from_secs(3), async {
            while destination.bytes_written() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the worker must dispatch before shutdown");
        drop(peer);

        let first_shutdown = forwarder.shutdown(Duration::ZERO).await;
        assert!(
            matches!(first_shutdown, Err(GossipError::Timeout)),
            "shutdown must report grace expiry even after aborting work"
        );
        let repeated_shutdown = forwarder.shutdown(Duration::from_secs(1)).await;
        assert!(
            matches!(repeated_shutdown, Err(GossipError::Timeout)),
            "repeated shutdown calls must report the same terminal outcome"
        );

        writer.shutdown();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        source.shutdown().await;
        sink.shutdown().await;
    }
}
