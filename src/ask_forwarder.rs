use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::pin::Pin;
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

#[cfg(test)]
struct AsyncPause {
    entered: Notify,
    release: Notify,
    used: AtomicBool,
}

#[cfg(test)]
impl AsyncPause {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            release: Notify::new(),
            used: AtomicBool::new(false),
        })
    }

    async fn wait_until_entered(&self) {
        self.entered.notified().await;
    }

    async fn pause(&self) {
        if self.used.swap(true, Ordering::AcqRel) {
            return;
        }
        self.entered.notify_one();
        self.release.notified().await;
    }

    fn release(&self) {
        self.release.notify_one();
    }
}

#[cfg(test)]
struct SyncPause {
    entered: Notify,
    released: std::sync::Condvar,
    state: Mutex<bool>,
}

#[cfg(test)]
impl SyncPause {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            released: std::sync::Condvar::new(),
            state: Mutex::new(false),
        })
    }

    async fn wait_until_entered(&self) {
        self.entered.notified().await;
    }

    fn pause(&self) {
        self.entered.notify_one();
        let mut released = self.state.lock().unwrap_or_else(|err| err.into_inner());
        while !*released {
            released = self
                .released
                .wait(released)
                .unwrap_or_else(|err| err.into_inner());
        }
    }

    fn release(&self) {
        let mut released = self.state.lock().unwrap_or_else(|err| err.into_inner());
        *released = true;
        self.released.notify_one();
    }
}

#[cfg(test)]
#[derive(Default)]
struct LifecycleTestHooks {
    leader_after_claim: Option<Arc<AsyncPause>>,
    waiting_before_registration: Option<Arc<AsyncPause>>,
    after_forced_abort: Option<Arc<AsyncPause>>,
    join_before_await: Option<Arc<AsyncPause>>,
    worker_exit_after_remaining: Option<Arc<SyncPause>>,
    reconciliation_after_claim: Option<Arc<SyncPause>>,
    ownership_snapshot: Option<Arc<AsyncPause>>,
}

struct WorkerControl {
    closed: AtomicBool,
    remaining: AtomicUsize,
    done: Notify,
    #[cfg(test)]
    test_hooks: Mutex<Option<Arc<LifecycleTestHooks>>>,
    // Count every task admitted before closure and every terminal outcome
    // observed by the worker. Forced cancellation accounts for the gap so a
    // dropped queued/in-flight task cannot silently disappear from metrics.
    admitted: AtomicUsize,
    completed: AtomicUsize,
    forced: AtomicBool,
    abnormal: AtomicBool,
    observer: Option<Arc<dyn AskForwardObserver>>,
    // A worker can be parked in `rx.recv()` while admission is open. The
    // shutdown path uses this edge-triggered wake in addition to the atomic
    // state so closure is observed without waiting for grace to elapse.
    shutdown_wake: Notify,
    shutdown: Mutex<ShutdownState>,
    shutdown_done: Notify,
    reconciliation: Mutex<ObserverReconciliation>,
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
            #[cfg(test)]
            test_hooks: Mutex::new(None),
            admitted: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            forced: AtomicBool::new(false),
            abnormal: AtomicBool::new(false),
            observer: completion_observer.clone(),
            shutdown_wake: Notify::new(),
            shutdown: Mutex::new(ShutdownState {
                terminal: None,
                leader: false,
                reclamation_complete: false,
            }),
            shutdown_done: Notify::new(),
            reconciliation: Mutex::new(ObserverReconciliation { complete: false }),
        });

        let mut worker_senders = Vec::with_capacity(workers);
        let mut abort_handles = Vec::with_capacity(workers);
        let mut joins = Vec::with_capacity(workers);
        for _ in 0..workers {
            let (tx, rx) = mpsc::channel::<ForwardTask>(capacity);
            let worker_observer = completion_observer.clone();
            let worker_control = control.clone();
            let worker_control_for_run = worker_control.clone();
            let worker_exit = WorkerExit {
                control: worker_control.clone(),
            };
            let handle = tokio::spawn(async move {
                let result = std::panic::AssertUnwindSafe(run_forward_worker(
                    rx,
                    worker_observer,
                    worker_control_for_run,
                    max_inflight,
                ))
                .catch_unwind()
                .await;
                if result.is_err() {
                    worker_control.abnormal.store(true, Ordering::Release);
                }
                drop(worker_exit);
            });
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

    #[cfg(test)]
    fn new_with_test_hooks(
        workers: usize,
        capacity: usize,
        completion_observer: Option<Arc<dyn AskForwardObserver>>,
        hooks: Arc<LifecycleTestHooks>,
    ) -> Self {
        let forwarder = Self::new_with_observer(workers, capacity, completion_observer);
        *forwarder
            .inner
            .control
            .test_hooks
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = Some(hooks);
        forwarder
    }

    /// Close admission, drain until `grace`, then abort remaining work and
    /// wait for workers to finish. Repeated calls observe the same terminal
    /// state. A timeout is returned as [`GossipError::Timeout`] rather than
    /// success.
    pub async fn shutdown(&self, grace: Duration) -> Result<()> {
        let control = Arc::clone(&self.inner.control);
        let terminal = loop {
            match claim_shutdown(&control) {
                ShutdownClaim::Complete(result) => return result.into_result(),
                ShutdownClaim::Leader(terminal) => break terminal,
                ShutdownClaim::Waiting => {
                    #[cfg(test)]
                    if let Some(hook) = lifecycle_test_hook(&control, |hooks| {
                        hooks.waiting_before_registration.clone()
                    }) {
                        hook.pause().await;
                    }
                }
            }

            // Register before rechecking leadership. If the leader is
            // cancelled between these operations, its drop notification is
            // retained by Notify and this caller will take over rather than
            // sleeping forever on a lost wake.
            let notified = control.shutdown_done.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match check_shutdown_wait(&control) {
                ShutdownWait::Complete(result) => return result.into_result(),
                ShutdownWait::Retry => continue,
                ShutdownWait::Wait => notified.await,
            }
        };

        let mut leader = ShutdownLeader {
            control: control.clone(),
            finished: false,
        };
        #[cfg(test)]
        if let Some(hook) = lifecycle_test_hook(&control, |hooks| hooks.leader_after_claim.clone())
        {
            hook.pause().await;
        }
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

        if terminal.is_none() && wait_for_workers(&control, grace).await {
            let abnormal = control.abnormal.load(Ordering::Acquire);
            let missing = finalize_observer_gaps(&control);
            if abnormal || missing {
                // A worker that exited abnormally may have dropped admitted
                // tasks. Do not publish a false Drained state.
                control.forced.store(true, Ordering::Release);
                set_shutdown_result(&control, ShutdownResult::TimedOut);
                reap_join_handles(&self.inner).await;
                finish_shutdown(&control, ShutdownResult::TimedOut);
                leader.finished = true;
                return Err(GossipError::Timeout);
            }
            reap_join_handles(&self.inner).await;
            finish_shutdown(&control, ShutdownResult::Drained);
            leader.finished = true;
            return Ok(());
        }

        // Persist the forced terminal reason before aborting or awaiting any
        // worker. The reason is sticky, but reclamation remains incomplete;
        // concurrent callers must await that separate completion state.
        control.forced.store(true, Ordering::Release);
        set_shutdown_result(&control, ShutdownResult::TimedOut);
        for handle in &self.inner.abort_handles {
            handle.abort();
        }
        #[cfg(test)]
        if let Some(hook) = lifecycle_test_hook(&control, |hooks| hooks.after_forced_abort.clone())
        {
            hook.pause().await;
        }
        wait_for_all_workers(&control).await;
        finalize_observer_gaps(&control);
        reap_join_handles(&self.inner).await;
        finish_shutdown(&control, ShutdownResult::TimedOut);
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
        // Count before sending so a worker that runs immediately cannot
        // publish completion before admission is visible to shutdown.
        self.inner.control.admitted.fetch_add(1, Ordering::AcqRel);
        let worker_count = self.inner.workers.len();
        let worker_idx = self.inner.next_worker.fetch_add(1, Ordering::Relaxed) % worker_count;
        match self.inner.workers[worker_idx].try_send(task) {
            Ok(()) => Ok(()),
            Err(err) => {
                self.inner.control.admitted.fetch_sub(1, Ordering::AcqRel);
                Err(match err {
                    mpsc::error::TrySendError::Full(_) => GossipError::WriteQueueFull,
                    mpsc::error::TrySendError::Closed(_) => GossipError::Shutdown,
                })
            }
        }
    }
}

impl Drop for AskForwarderInner {
    fn drop(&mut self) {
        self.control.closed.store(true, Ordering::Release);
        self.control.forced.store(true, Ordering::Release);
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
            let mut state = self
                .control
                .shutdown
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            state.leader = false;
            drop(state);
            self.control.shutdown_done.notify_waiters();
        }
    }
}

#[derive(Clone, Copy)]
struct ShutdownState {
    terminal: Option<ShutdownResult>,
    leader: bool,
    reclamation_complete: bool,
}

enum ShutdownClaim {
    Complete(ShutdownResult),
    Leader(Option<ShutdownResult>),
    Waiting,
}

enum ShutdownWait {
    Complete(ShutdownResult),
    Retry,
    Wait,
}

fn claim_shutdown(control: &WorkerControl) -> ShutdownClaim {
    let mut state = control
        .shutdown
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    if state.reclamation_complete {
        return ShutdownClaim::Complete(
            state
                .terminal
                .expect("reclamation completion requires a terminal result"),
        );
    }
    if state.leader {
        ShutdownClaim::Waiting
    } else {
        state.leader = true;
        ShutdownClaim::Leader(state.terminal)
    }
}

fn check_shutdown_wait(control: &WorkerControl) -> ShutdownWait {
    let state = control
        .shutdown
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    if state.reclamation_complete {
        ShutdownWait::Complete(
            state
                .terminal
                .expect("reclamation completion requires a terminal result"),
        )
    } else if !state.leader {
        ShutdownWait::Retry
    } else {
        ShutdownWait::Wait
    }
}

struct ObserverReconciliation {
    complete: bool,
}

fn set_shutdown_result(control: &WorkerControl, result: ShutdownResult) {
    let mut state = control
        .shutdown
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    if state.terminal.is_none() {
        state.terminal = Some(result);
    }
}

fn finish_shutdown(control: &WorkerControl, result: ShutdownResult) {
    let mut state = control
        .shutdown
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    if state.terminal.is_none() {
        state.terminal = Some(result);
    }
    state.reclamation_complete = true;
    drop(state);
    control.shutdown_done.notify_waiters();
}

async fn wait_for_all_workers(control: &WorkerControl) {
    loop {
        let notified = control.done.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if control.remaining.load(Ordering::Acquire) == 0 {
            return;
        }
        notified.await;
    }
}

struct JoinHandleReaper<'a> {
    inner: &'a AskForwarderInner,
    joins: Vec<JoinHandle<()>>,
    complete: bool,
}

impl Drop for JoinHandleReaper<'_> {
    fn drop(&mut self) {
        if self.complete || self.joins.is_empty() {
            return;
        }
        let mut guard = self
            .inner
            .joins
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        guard.append(&mut self.joins);
    }
}

async fn reap_join_handles(inner: &AskForwarderInner) {
    let joins = {
        let mut guard = inner.joins.lock().unwrap_or_else(|err| err.into_inner());
        std::mem::take(&mut *guard)
    };
    let mut reaper = JoinHandleReaper {
        inner,
        joins,
        complete: false,
    };
    while !reaper.joins.is_empty() {
        #[cfg(test)]
        if let Some(hook) = lifecycle_test_hook(inner.control.as_ref(), |hooks| {
            hooks.join_before_await.clone()
        }) {
            hook.pause().await;
        }
        // Keep the handle in the guard until its poll is ready. If this
        // future is cancelled while the worker is still running, Drop can
        // restore that unfinished handle for the next shutdown leader. Remove
        // a completed handle immediately so it is never polled twice after a
        // cancellation between joins.
        let _ = poll_fn(|cx| Pin::new(&mut reaper.joins[0]).poll(cx)).await;
        reaper.joins.swap_remove(0);
    }
    reaper.complete = true;
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
        let was_last = self.control.remaining.fetch_sub(1, Ordering::AcqRel) == 1;
        if was_last {
            // Publish the worker-count transition before reconciliation so a
            // shutdown finalizer can contend for the reconciliation mutex.
            // That mutex makes this the only reconciliation owner, while
            // shutdown completion is still published only after callbacks
            // have returned.
            self.control.done.notify_waiters();
            #[cfg(test)]
            if let Some(hook) = lifecycle_test_hook(&self.control, |hooks| {
                hooks.worker_exit_after_remaining.clone()
            }) {
                hook.pause();
            }
            finalize_observer_gaps(&self.control);
        }
    }
}

#[cfg(test)]
fn lifecycle_test_hook<T>(
    control: &WorkerControl,
    select: impl FnOnce(&LifecycleTestHooks) -> Option<Arc<T>>,
) -> Option<Arc<T>> {
    control
        .test_hooks
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .as_deref()
        .and_then(select)
}

/// Reconcile accepted tasks whose worker futures were dropped by forced or
/// abnormal termination. The ownership mutex covers the read/callback/update
/// sequence, so a last-worker drop and shutdown finalizer cannot emit the same
/// missing error twice or publish completion before callbacks finish.
fn finalize_observer_gaps(control: &WorkerControl) -> bool {
    let mut reconciliation = control
        .reconciliation
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    if reconciliation.complete {
        return false;
    }
    let admitted = control.admitted.load(Ordering::Acquire);
    let completed = control.completed.load(Ordering::Acquire);
    let missing = admitted.saturating_sub(completed);
    #[cfg(test)]
    if let Some(hook) =
        lifecycle_test_hook(control, |hooks| hooks.reconciliation_after_claim.clone())
    {
        hook.pause();
    }
    if let Some(observer) = control.observer.as_deref() {
        for _ in 0..missing {
            observer.record_error();
        }
    }
    if missing != 0 {
        control.completed.fetch_add(missing, Ordering::AcqRel);
    }
    reconciliation.complete = true;
    missing != 0
}

async fn run_forward_worker(
    mut rx: mpsc::Receiver<ForwardTask>,
    worker_observer: Option<Arc<dyn AskForwardObserver>>,
    control: Arc<WorkerControl>,
    max_inflight: usize,
) {
    let mut inflight = FuturesUnordered::new();
    let mut waiting: VecDeque<ForwardTask> = VecDeque::new();
    let mut rx_closed = false;

    loop {
        // Register before checking `closed`: shutdown can race this loop
        // between the check and select. Keeping this same enabled future for
        // the select closes that registration gap for every worker.
        let shutdown_notified = control.shutdown_wake.notified();
        tokio::pin!(shutdown_notified);
        shutdown_notified.as_mut().enable();

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
        expire_waiting(&mut waiting, worker_observer.as_deref(), &control);
        dispatch_waiting(
            &mut waiting,
            &mut inflight,
            max_inflight,
            worker_observer.as_deref(),
            &control,
        );

        #[cfg(test)]
        if !waiting.is_empty()
            && !inflight.is_empty()
            && !rx_closed
            && !rx.is_empty()
            && let Some(hook) =
                lifecycle_test_hook(&control, |hooks| hooks.ownership_snapshot.clone())
        {
            hook.pause().await;
        }

        if rx_closed && waiting.is_empty() && inflight.is_empty() {
            break;
        }

        let earliest = earliest_deadline(&waiting);
        tokio::select! {
            maybe_task = rx.recv(), if !rx_closed => {
                match maybe_task {
                    Some(task) => waiting.push_back(task),
                    None => rx_closed = true,
                }
            }
            Some(completed) = inflight.next(), if !inflight.is_empty() => {
                match completed {
                    Some(outcome) => {
                        handle_completed_forward(outcome, worker_observer.as_deref(), &control);
                    }
                    None => {
                        // The isolated forward caught a panic. The worker
                        // survived, but this admitted task still needs one
                        // terminal error observation.
                        handle_completed_forward(
                            ForwardOutcome::Error,
                            worker_observer.as_deref(),
                            &control,
                        );
                    }
                }
            }
            _ = sleep_until_deadline(earliest), if earliest.is_some() => {}
            _ = &mut shutdown_notified => {}
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

fn expire_waiting(
    waiting: &mut VecDeque<ForwardTask>,
    observer: Option<&dyn AskForwardObserver>,
    control: &WorkerControl,
) {
    let now = Instant::now();
    let mut index = 0usize;
    while index < waiting.len() {
        if waiting[index]
            .deadline
            .is_some_and(|deadline| deadline <= now)
        {
            let task = waiting.remove(index).expect("index in range");
            complete_expired_queued(task, observer, control);
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
    control: &WorkerControl,
) {
    while inflight.len() < max_inflight {
        let Some(task) = waiting.pop_front() else {
            break;
        };
        if task
            .deadline
            .is_some_and(|deadline| deadline <= Instant::now())
        {
            complete_expired_queued(task, observer, control);
            continue;
        }
        inflight.push(Box::pin(run_forward_task_isolated(task)));
    }
}

fn complete_expired_queued(
    task: ForwardTask,
    observer: Option<&dyn AskForwardObserver>,
    control: &WorkerControl,
) {
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
    handle_completed_forward(outcome, observer, control);
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
            if deliver_forwarded_reply_outcome(task.responder, reply).await {
                success
            } else {
                ForwardOutcome::ReplyUndeliverable
            }
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
        // A duplicate claim only proves that a sibling owns the one-shot
        // responder guard. It does not prove that sibling delivered its
        // reply, so account this return path as undeliverable.
        Err(err) if is_duplicate_reply_claim(&err) => false,
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
    control: &WorkerControl,
) {
    control.completed.fetch_add(1, Ordering::AcqRel);
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_shutdown_leader_is_taken_over_without_a_third_caller() {
        let leader_after_claim = AsyncPause::new();
        let waiting_before_registration = AsyncPause::new();
        let hooks = Arc::new(LifecycleTestHooks {
            leader_after_claim: Some(leader_after_claim.clone()),
            waiting_before_registration: Some(waiting_before_registration.clone()),
            ..Default::default()
        });
        let forwarder = AskForwarder::new_with_test_hooks(1, 128, None, hooks);

        let leader = tokio::spawn({
            let forwarder = forwarder.clone();
            async move { forwarder.shutdown(Duration::ZERO).await }
        });
        tokio::time::timeout(
            Duration::from_secs(2),
            leader_after_claim.wait_until_entered(),
        )
        .await
        .expect("leader must reach the cancellable post-claim interleaving");

        let follower = tokio::spawn({
            let forwarder = forwarder.clone();
            async move { forwarder.shutdown(Duration::from_secs(1)).await }
        });
        tokio::time::timeout(
            Duration::from_secs(2),
            waiting_before_registration.wait_until_entered(),
        )
        .await
        .expect("follower must reach the pre-registration interleaving");

        // Cancel the only leader while the follower has not registered its
        // notification. The follower must recheck leadership after registering
        // and take over; no third caller is allowed to provide the wake.
        leader.abort();
        assert!(
            leader.await.is_err(),
            "the first shutdown must be cancelled"
        );
        waiting_before_registration.release();
        assert!(
            tokio::time::timeout(Duration::from_secs(2), follower)
                .await
                .expect("the follower must not strand after leader cancellation")
                .expect("the follower task must not panic")
                .is_ok()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_post_abort_shutdown_waits_for_worker_reclamation() {
        let after_forced_abort = AsyncPause::new();
        let waiting_before_registration = AsyncPause::new();
        let worker_exit_after_remaining = SyncPause::new();
        let hooks = Arc::new(LifecycleTestHooks {
            after_forced_abort: Some(after_forced_abort.clone()),
            waiting_before_registration: Some(waiting_before_registration.clone()),
            worker_exit_after_remaining: Some(worker_exit_after_remaining.clone()),
            ..Default::default()
        });
        let observer = Arc::new(LifecycleObserver::default());
        let (destination, source, sink) = test_destination().await;
        let (io, peer) = tokio::io::duplex(65536);
        let (writer, writer_task, _) = LockFreeStreamHandle::new(
            io,
            test_addr(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let writer = Arc::new(writer);
        let forwarder = AskForwarder::new_with_test_hooks(1, 128, Some(observer.clone()), hooks);
        forwarder
            .try_forward_actor_ask_no_timeout(
                destination.clone(),
                1,
                1,
                Bytes::from_static(b"post-abort"),
                AskResponder::from_stream_handle(
                    12,
                    writer.clone(),
                    Arc::new(AtomicBool::new(false)),
                ),
            )
            .expect("the in-flight task must be admitted");
        tokio::time::timeout(Duration::from_secs(3), async {
            while destination.bytes_written() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the task must reach the in-flight destination call");

        let leader = tokio::spawn({
            let forwarder = forwarder.clone();
            async move { forwarder.shutdown(Duration::ZERO).await }
        });
        tokio::time::timeout(
            Duration::from_secs(2),
            after_forced_abort.wait_until_entered(),
        )
        .await
        .expect("shutdown must persist timeout before reclamation");
        tokio::time::timeout(
            Duration::from_secs(2),
            worker_exit_after_remaining.wait_until_entered(),
        )
        .await
        .expect("the aborted worker must reach its reclamation interleaving");

        let mut follower = tokio::spawn({
            let forwarder = forwarder.clone();
            async move { forwarder.shutdown(Duration::from_secs(1)).await }
        });
        tokio::time::timeout(
            Duration::from_secs(2),
            waiting_before_registration.wait_until_entered(),
        )
        .await
        .expect("the concurrent caller must observe the active leader");

        after_forced_abort.release();
        let observed_reconciliation = tokio::time::timeout(Duration::from_secs(2), async {
            while observer.error.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();
        // Release the follower's own registration gate before checking that it
        // remains pending; otherwise the timeout would only prove that the
        // test hook is still holding the follower.
        waiting_before_registration.release();
        let follower_still_waiting =
            tokio::time::timeout(Duration::from_millis(100), &mut follower)
                .await
                .is_err();

        worker_exit_after_remaining.release();
        assert!(
            observed_reconciliation,
            "shutdown must reconcile before joining"
        );
        assert!(
            follower_still_waiting,
            "a post-abort caller must await reclamation after registering"
        );
        assert!(matches!(
            leader.await.expect("leader task must not panic"),
            Err(GossipError::Timeout)
        ));
        assert!(matches!(
            follower.await.expect("follower task must not panic"),
            Err(GossipError::Timeout)
        ));
        assert_eq!(observer.error.load(Ordering::SeqCst), 1);
        assert_eq!(forwarder.inner.control.completed.load(Ordering::SeqCst), 1);
        assert_eq!(forwarder.inner.permits.available_permits(), 144);

        drop(peer);
        writer.shutdown();
        tokio::time::timeout(Duration::from_secs(2), writer_task)
            .await
            .expect("writer task must be reclaimed")
            .expect("writer task must not panic");
        source.shutdown().await;
        sink.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelled_shutdown_join_owner_keeps_paused_last_worker_for_successor() {
        let worker_exit_after_remaining = SyncPause::new();
        let join_before_await = AsyncPause::new();
        let waiting_before_registration = AsyncPause::new();
        let hooks = Arc::new(LifecycleTestHooks {
            waiting_before_registration: Some(waiting_before_registration.clone()),
            join_before_await: Some(join_before_await.clone()),
            worker_exit_after_remaining: Some(worker_exit_after_remaining.clone()),
            ..Default::default()
        });
        let forwarder = AskForwarder::new_with_test_hooks(1, 128, None, hooks);

        let leader_cancel = Arc::new(Notify::new());
        let (leader_done_tx, leader_done_rx) = std::sync::mpsc::channel();
        let leader_thread = {
            let forwarder = forwarder.clone();
            let leader_cancel = leader_cancel.clone();
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("shutdown executor must build");
                runtime.block_on(async move {
                    tokio::select! {
                        result = forwarder.shutdown(Duration::from_millis(10)) => {
                            leader_done_tx.send(Some(result)).expect("leader result receiver");
                        }
                        _ = leader_cancel.notified() => {
                            leader_done_tx.send(None).expect("leader cancellation receiver");
                        }
                    }
                });
            })
        };
        tokio::time::timeout(
            Duration::from_secs(2),
            worker_exit_after_remaining.wait_until_entered(),
        )
        .await
        .expect("the last worker must decrement before join reclamation");
        assert_eq!(forwarder.inner.control.remaining.load(Ordering::SeqCst), 0);
        tokio::time::timeout(
            Duration::from_secs(2),
            join_before_await.wait_until_entered(),
        )
        .await
        .expect("the leader must enter join await while the worker is paused");

        let mut follower = tokio::spawn({
            let forwarder = forwarder.clone();
            async move { forwarder.shutdown(Duration::ZERO).await }
        });
        tokio::time::timeout(
            Duration::from_secs(2),
            waiting_before_registration.wait_until_entered(),
        )
        .await
        .expect("the successor must reach its registration gate");

        // Cancellation drops the only active shutdown leader after it has
        // claimed the join handle, so a successor must inherit that handle.
        leader_cancel.notify_one();
        assert!(
            leader_done_rx
                .recv()
                .expect("leader completion receiver")
                .is_none(),
            "the join owner must be cancelled"
        );
        leader_thread
            .join()
            .expect("shutdown executor thread must not panic");

        // This release is deliberately before the noncompletion assertion:
        // the follower must be waiting on the retained worker join, not its
        // own registration hook.
        waiting_before_registration.release();
        let returned_while_worker_paused =
            tokio::time::timeout(Duration::from_millis(100), &mut follower)
                .await
                .is_ok();
        worker_exit_after_remaining.release();

        assert!(
            !returned_while_worker_paused,
            "the successor must not publish completion before the paused worker is joined"
        );
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), follower)
                .await
                .expect("successor must finish after worker release")
                .expect("successor task must not panic"),
            Ok(())
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn last_worker_and_shutdown_reconcile_observer_gap_exactly_once() {
        let worker_exit_after_remaining = SyncPause::new();
        let reconciliation_after_claim = SyncPause::new();
        let hooks = Arc::new(LifecycleTestHooks {
            worker_exit_after_remaining: Some(worker_exit_after_remaining.clone()),
            reconciliation_after_claim: Some(reconciliation_after_claim.clone()),
            ..Default::default()
        });
        let observer = Arc::new(LifecycleObserver::default());
        let (destination, source, sink) = test_destination().await;
        let (io, peer) = tokio::io::duplex(65536);
        let (writer, writer_task, _) = LockFreeStreamHandle::new(
            io,
            test_addr(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let writer = Arc::new(writer);
        let forwarder = AskForwarder::new_with_test_hooks(1, 128, Some(observer.clone()), hooks);
        forwarder
            .try_forward_actor_ask_no_timeout(
                destination.clone(),
                1,
                1,
                Bytes::from_static(b"reconcile-once"),
                AskResponder::from_stream_handle(
                    13,
                    writer.clone(),
                    Arc::new(AtomicBool::new(false)),
                ),
            )
            .expect("the in-flight task must be admitted");
        tokio::time::timeout(Duration::from_secs(3), async {
            while destination.bytes_written() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the task must reach the in-flight destination call");

        let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
        let shutdown_forwarder = forwarder.clone();
        let shutdown_thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("shutdown executor must build");
            let result = runtime.block_on(shutdown_forwarder.shutdown(Duration::ZERO));
            shutdown_tx.send(result).expect("shutdown result receiver");
        });
        tokio::time::timeout(
            Duration::from_secs(2),
            worker_exit_after_remaining.wait_until_entered(),
        )
        .await
        .expect("the last worker must decrement before reconciliation");
        tokio::time::timeout(
            Duration::from_secs(2),
            reconciliation_after_claim.wait_until_entered(),
        )
        .await
        .expect("shutdown must contend for reconciliation while the worker is exiting");
        assert_eq!(observer.error.load(Ordering::SeqCst), 0);
        assert_eq!(forwarder.inner.control.completed.load(Ordering::SeqCst), 0);

        // Let shutdown own and finish the callback/update sequence, then let
        // the last worker run its second reconciliation attempt.
        reconciliation_after_claim.release();
        tokio::time::timeout(Duration::from_secs(2), async {
            while observer.error.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the single reconciliation owner must account for the gap");
        worker_exit_after_remaining.release();

        let shutdown_result =
            tokio::task::spawn_blocking(move || shutdown_rx.recv().expect("shutdown result"))
                .await
                .expect("shutdown result waiter must not panic");
        assert!(matches!(shutdown_result, Err(GossipError::Timeout)));
        shutdown_thread
            .join()
            .expect("shutdown executor thread must not panic");
        assert_eq!(observer.error.load(Ordering::SeqCst), 1);
        assert_eq!(forwarder.inner.control.completed.load(Ordering::SeqCst), 1);

        drop(peer);
        writer.shutdown();
        tokio::time::timeout(Duration::from_secs(2), writer_task)
            .await
            .expect("writer task must be reclaimed")
            .expect("writer task must not panic");
        source.shutdown().await;
        sink.shutdown().await;
    }

    #[derive(Default)]
    struct LifecycleObserver {
        success: AtomicUsize,
        error: AtomicUsize,
    }

    impl AskForwardObserver for LifecycleObserver {
        fn record_success(&self) {
            self.success.fetch_add(1, Ordering::SeqCst);
        }

        fn record_error(&self) {
            self.error.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_accounts_simultaneous_queued_waiting_and_inflight_ownership() {
        let ownership_snapshot = AsyncPause::new();
        let hooks = Arc::new(LifecycleTestHooks {
            ownership_snapshot: Some(ownership_snapshot.clone()),
            ..Default::default()
        });
        let observer = Arc::new(LifecycleObserver::default());
        let (destination, source, sink) = test_destination().await;
        let (io, peer) = tokio::io::duplex(65536);
        let (writer, writer_task, _) = LockFreeStreamHandle::new(
            io,
            test_addr(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let writer = Arc::new(writer);
        let forwarder = AskForwarder::new_with_test_hooks(1, 128, Some(observer.clone()), hooks);
        const ADMITTED: usize = 20;
        for index in 0..ADMITTED {
            forwarder
                .try_forward_actor_ask_no_timeout(
                    destination.clone(),
                    1,
                    1,
                    Bytes::from_static(b"ownership"),
                    AskResponder::from_stream_handle(
                        100 + index as u32,
                        writer.clone(),
                        Arc::new(AtomicBool::new(false)),
                    ),
                )
                .expect("all ownership-probe tasks must be admitted");
        }

        // The worker hook is reached only after at least one task is queued in
        // the receiver, one is waiting behind MAX_INFLIGHT_PER_WORKER, and
        // one is owned by the in-flight set at the same instant.
        tokio::time::timeout(
            Duration::from_secs(3),
            ownership_snapshot.wait_until_entered(),
        )
        .await
        .expect("all three worker ownership locations must coexist");
        assert_eq!(
            forwarder.inner.control.admitted.load(Ordering::SeqCst),
            ADMITTED
        );

        let shutdown = tokio::spawn({
            let forwarder = forwarder.clone();
            async move { forwarder.shutdown(Duration::ZERO).await }
        });
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(3), shutdown)
                .await
                .expect("forced ownership reconciliation must finish")
                .expect("shutdown task must not panic"),
            Err(GossipError::Timeout)
        ));
        assert_eq!(observer.success.load(Ordering::SeqCst), 0);
        assert_eq!(observer.error.load(Ordering::SeqCst), ADMITTED);
        assert_eq!(
            forwarder.inner.control.completed.load(Ordering::SeqCst),
            ADMITTED
        );
        assert_eq!(forwarder.inner.permits.available_permits(), 144);

        drop(peer);
        writer.shutdown();
        tokio::time::timeout(Duration::from_secs(2), writer_task)
            .await
            .expect("writer task must be reclaimed")
            .expect("writer task must not panic");
        source.shutdown().await;
        sink.shutdown().await;
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
