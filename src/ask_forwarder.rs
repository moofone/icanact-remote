use std::future::{Future, poll_fn};
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use bytes::Bytes;
use crossbeam_queue::ArrayQueue;
use futures::FutureExt;
use futures::task::AtomicWaker;
use tokio::sync::Notify;
use tokio::task::{AbortHandle, JoinHandle};

use crate::ask_responder::{ForwardReplyAdmission, ForwardReplyToken, ReplySession};
use crate::{AskResponder, GossipError, RemoteConnection, Result};

/// A complete payload owner with conservative retained-capacity accounting.
///
/// Vec, boxed, static, aligned and pooled owners remain inline and unique.
/// Moving them into forwarding or reply admission adds no owner-wrapper
/// allocation or reference-count operation. Arbitrary `Bytes` views cannot
/// establish retained capacity and are deliberately not accepted.
pub struct ForwardPayload {
    owner: ForwardPayloadOwner,
    offset: usize,
    len: usize,
    retained_bytes: usize,
}

enum ForwardPayloadOwner {
    Vector(Vec<u8>),
    Boxed(Box<[u8]>),
    Static(&'static [u8]),
    Aligned(crate::AlignedBytes),
    Pooled(crate::typed::NativePooledOwner),
    // Sharing is an explicit caller choice, never an implicit Clone fallback.
    Shared(Arc<ForwardPayload>),
    #[cfg(test)]
    TestOwner(Bytes),
}

impl AsRef<[u8]> for ForwardPayloadOwner {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Vector(bytes) => bytes,
            Self::Boxed(bytes) => bytes,
            Self::Static(bytes) => bytes,
            Self::Aligned(bytes) => bytes.as_ref(),
            Self::Pooled(bytes) => bytes.as_ref(),
            Self::Shared(bytes) => bytes.as_ref().as_ref(),
            #[cfg(test)]
            Self::TestOwner(bytes) => bytes.as_ref(),
        }
    }
}

impl ForwardPayload {
    #[cfg(test)]
    pub(crate) fn from_test_owner<T>(owner: T, retained_bytes: usize) -> Self
    where
        T: AsRef<[u8]> + Send + Sync + 'static,
    {
        let len = owner.as_ref().len();
        assert!(retained_bytes >= len);
        Self {
            owner: ForwardPayloadOwner::TestOwner(Bytes::from_owner(owner)),
            offset: 0,
            len,
            retained_bytes,
        }
    }

    pub(crate) fn from_native_pooled(
        owner: crate::typed::NativePooledOwner,
        offset: usize,
        len: usize,
    ) -> Self {
        let retained_bytes = owner.retained_bytes();
        Self {
            owner: ForwardPayloadOwner::Pooled(owner),
            offset,
            len,
            retained_bytes,
        }
    }

    /// Flatten a trusted pooled `Buf` cursor without promoting the owner.
    pub(crate) fn remaining_view(mut self, offset: usize, len: usize) -> Self {
        assert!(offset <= self.len && len <= self.len - offset);
        self.offset += offset;
        self.len = len;
        self
    }

    /// Full capacity held by the proven owner, not merely its visible length.
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.as_ref()
    }

    /// Explicit compatibility promotion to ref-counted `Bytes`. This may
    /// allocate owner metadata; forwarding/reply writer paths retain Self.
    pub fn into_bytes(self) -> Bytes {
        Bytes::from_owner(self)
    }

    /// Explicit fanout setup. This allocates one shared owner wrapper; ordinary
    /// forwarding never calls it. Subsequent aliases use `shared_clone`.
    pub fn into_shared(self) -> Self {
        if matches!(&self.owner, ForwardPayloadOwner::Shared(_)) {
            return self;
        }
        let len = self.len;
        let retained_bytes = self.retained_bytes;
        Self {
            owner: ForwardPayloadOwner::Shared(Arc::new(self)),
            offset: 0,
            len,
            retained_bytes,
        }
    }

    /// Alias an explicitly shared owner, preserving its full capacity receipt.
    /// Unique owners are rejected rather than silently copying or promoting.
    pub fn shared_clone(&self) -> Result<Self> {
        let ForwardPayloadOwner::Shared(owner) = &self.owner else {
            return Err(GossipError::InvalidConfig(
                "payload must be explicitly shared before creating an alias".into(),
            ));
        };
        Ok(Self {
            owner: ForwardPayloadOwner::Shared(Arc::clone(owner)),
            offset: self.offset,
            len: self.len,
            retained_bytes: self.retained_bytes,
        })
    }

    pub fn from_static(bytes: &'static [u8]) -> Self {
        Self {
            owner: ForwardPayloadOwner::Static(bytes),
            offset: 0,
            len: bytes.len(),
            retained_bytes: bytes.len(),
        }
    }

    /// Explicit copying convenience constructor. Move an existing owner with
    /// `From`/`TryFrom` when its allocation should be preserved instead.
    pub fn copy_from_slice(bytes: &[u8]) -> Self {
        Self::from(bytes.to_vec())
    }
}

impl AsRef<[u8]> for ForwardPayload {
    fn as_ref(&self) -> &[u8] {
        &self.owner.as_ref()[self.offset..self.offset + self.len]
    }
}

impl From<Vec<u8>> for ForwardPayload {
    fn from(bytes: Vec<u8>) -> Self {
        let len = bytes.len();
        let retained_bytes = bytes.capacity();
        Self {
            owner: ForwardPayloadOwner::Vector(bytes),
            offset: 0,
            len,
            retained_bytes,
        }
    }
}

impl From<Box<[u8]>> for ForwardPayload {
    fn from(bytes: Box<[u8]>) -> Self {
        let len = bytes.len();
        Self {
            owner: ForwardPayloadOwner::Boxed(bytes),
            offset: 0,
            len,
            retained_bytes: len,
        }
    }
}

impl From<crate::typed::PooledPayload> for ForwardPayload {
    fn from(payload: crate::typed::PooledPayload) -> Self {
        payload.into_forward_payload()
    }
}

impl TryFrom<crate::AlignedBytes> for ForwardPayload {
    type Error = GossipError;

    fn try_from(payload: crate::AlignedBytes) -> Result<Self> {
        let retained_bytes = payload.retained_capacity().ok_or_else(|| {
            GossipError::InvalidConfig(
                "forwarding requires a proven aligned owner or an explicitly owned payload".into(),
            )
        })?;
        let len = payload.len();
        Ok(Self {
            owner: ForwardPayloadOwner::Aligned(payload),
            offset: 0,
            len,
            retained_bytes,
        })
    }
}

/// Opaque receipt for the one physical destination bound to a forwarder.
/// Copying a receipt never retains a transport and cannot retarget it by address.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ForwardDestination {
    endpoint_id: u64,
    session_id: u64,
}

struct BoundForwardDestination {
    token: ForwardDestination,
    connection: RemoteConnection,
}

struct ForwardEndpoints {
    endpoint_id: Option<u64>,
    destination: OnceLock<BoundForwardDestination>,
    reply: OnceLock<ReplySession>,
    reply_initializing: AtomicBool,
    reply_capacity: usize,
}

impl ForwardEndpoints {
    fn new(reply_capacity: usize) -> Self {
        static NEXT_ENDPOINT: AtomicU64 = AtomicU64::new(1);
        Self {
            // Endpoint identities are never reissued, including after drop.
            // Exhaustion rejects cold binding rather than wrapping or panicking.
            endpoint_id: NEXT_ENDPOINT
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
                .ok(),
            destination: OnceLock::new(),
            reply: OnceLock::new(),
            reply_initializing: AtomicBool::new(false),
            reply_capacity,
        }
    }

    fn bind(&self, connection: RemoteConnection) -> Result<ForwardDestination> {
        let endpoint_id = self.endpoint_id.ok_or(GossipError::WriteQueueFull)?;
        let session_id = connection.instance_id().ok_or(GossipError::Shutdown)?;
        let token = ForwardDestination {
            endpoint_id,
            session_id,
        };
        self.destination
            .set(BoundForwardDestination { token, connection })
            .map_err(|_| GossipError::WriteQueueFull)?;
        Ok(token)
    }

    fn reply_session(&self, responder: &AskResponder<'_>) -> Result<&ReplySession> {
        if let Some(session) = self.reply.get() {
            return Ok(session);
        }
        self.reply_initializing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| GossipError::WriteQueueFull)?;
        struct BindingClaim<'a>(&'a AtomicBool);
        impl Drop for BindingClaim<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let _binding = BindingClaim(&self.reply_initializing);
        if self.reply.get().is_none() {
            let session = responder.reply_session_with_capacity(self.reply_capacity)?;
            let _ = self.reply.set(session);
        }
        self.reply.get().ok_or(GossipError::Shutdown)
    }

    fn accepts(&self, token: ForwardDestination) -> bool {
        self.destination
            .get()
            .is_some_and(|destination| destination.token == token)
    }
}

struct ForwardTask {
    actor_id: u64,
    type_hash: u32,
    payload: ForwardPayload,
    responder: Option<ForwardReplyToken>,
    deadline: Option<Instant>,
    timeout_reply: Option<ForwardPayload>,
    error_reply: Option<ForwardPayload>,
    retained_bytes: usize,
}

/// Both counters are preallocated. Failed compare-exchange is bounded local
/// backpressure rather than a retry loop or a hidden waiter allocation.
struct ForwardBudget {
    count: AtomicUsize,
    bytes: AtomicUsize,
}

impl ForwardBudget {
    fn new(count: usize, bytes: usize) -> Self {
        Self {
            count: AtomicUsize::new(count),
            bytes: AtomicUsize::new(bytes),
        }
    }

    fn take(counter: &AtomicUsize, amount: usize) -> Result<()> {
        let available = counter.load(Ordering::Acquire);
        let remaining = available
            .checked_sub(amount)
            .ok_or(GossipError::WriteQueueFull)?;
        counter
            .compare_exchange(available, remaining, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| GossipError::WriteQueueFull)?;
        Ok(())
    }

    fn try_acquire(&self, bytes: usize) -> Result<ForwardPermit<'_>> {
        Self::take(&self.count, 1)?;
        if let Err(error) = Self::take(&self.bytes, bytes) {
            self.count.fetch_add(1, Ordering::Release);
            return Err(error);
        }
        Ok(ForwardPermit {
            budget: self,
            bytes,
            armed: true,
            reply: None,
        })
    }

    fn release(&self, bytes: usize) {
        self.bytes.fetch_add(bytes, Ordering::Release);
        self.count.fetch_add(1, Ordering::Release);
    }
}

/// Borrowed only within a publisher or receiver's scope. No per-job shared
/// owner, semaphore wait node, or allocation is needed. Queued jobs transfer
/// the numeric charge; receivers reconstruct the guard without reacquiring.
struct ForwardPermit<'a> {
    budget: &'a ForwardBudget,
    bytes: usize,
    armed: bool,
    reply: Option<(&'a ReplySession, ForwardReplyToken)>,
}

impl Drop for ForwardPermit<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.budget.release(self.bytes);
            if let Some((session, token)) = self.reply {
                session.cancel(token);
            }
        }
    }
}

struct ForwardQueue {
    queue: ArrayQueue<ForwardTask>,
    budget: Arc<ForwardBudget>,
    endpoints: Arc<ForwardEndpoints>,
    closed: AtomicBool,
    ready: AtomicWaker,
}

impl ForwardQueue {
    fn new(capacity: usize, budget: Arc<ForwardBudget>, endpoints: Arc<ForwardEndpoints>) -> Self {
        Self {
            queue: ArrayQueue::new(capacity),
            budget,
            endpoints,
            closed: AtomicBool::new(false),
            ready: AtomicWaker::new(),
        }
    }

    fn try_send(
        &self,
        task: ForwardTask,
        mut permit: ForwardPermit<'_>,
        admission: Option<ForwardReplyAdmission<'_>>,
    ) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            drop(task);
            return Err(GossipError::Shutdown);
        }
        self.queue
            .push(task)
            .map_err(|_| GossipError::WriteQueueFull)?;
        // Transfer before invoking any waker or dropping a queued payload:
        // either can unwind, and the queue now owns the sole budget return.
        permit.armed = false;
        if let Some(admission) = admission {
            admission.commit();
        }
        // A canceled receiver may close/drain between our precheck and push.
        // The final producer completes that drain; each pop returns one charge.
        // Publication is still accepted and shutdown records its terminal error.
        if self.closed.load(Ordering::Acquire) {
            self.drain();
        }
        self.ready.wake();
        Ok(())
    }

    fn drain(&self) {
        while let Some(task) = self.queue.pop() {
            let _permit = ForwardPermit {
                budget: &self.budget,
                bytes: task.retained_bytes,
                armed: true,
                reply: self.endpoints.reply.get().zip(task.responder),
            };
            // One hostile destructor must not strand the remaining accepted
            // queue during forced reclamation. Its own charge is RAII-owned.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(task)));
        }
    }
}

impl Drop for ForwardQueue {
    fn drop(&mut self) {
        self.drain();
    }
}

struct ForwardReceiver(Arc<ForwardQueue>);

impl ForwardReceiver {
    fn try_recv(&self) -> Option<(ForwardTask, ForwardPermit<'_>)> {
        let task = self.0.queue.pop()?;
        let permit = ForwardPermit {
            budget: &self.0.budget,
            bytes: task.retained_bytes,
            armed: true,
            reply: self.0.endpoints.reply.get().zip(task.responder),
        };
        Some((task, permit))
    }

    async fn recv(&self) -> Option<(ForwardTask, ForwardPermit<'_>)> {
        poll_fn(|cx| {
            self.0.ready.register(cx.waker());
            match self.try_recv() {
                Some(task) => Poll::Ready(Some(task)),
                None if self.0.closed.load(Ordering::Acquire) => Poll::Ready(None),
                None => Poll::Pending,
            }
        })
        .await
    }
}

impl Drop for ForwardReceiver {
    fn drop(&mut self) {
        self.0.closed.store(true, Ordering::Release);
        self.0.drain();
    }
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
    publisher_before_enqueue: Option<Arc<SyncPause>>,
}

struct WorkerControl {
    closed: AtomicBool,
    // High bit closes publication; low bits count in-progress publishers.
    publishers: AtomicUsize,
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
    endpoints: Arc<ForwardEndpoints>,
    workers: Vec<Arc<ForwardQueue>>,
    next_worker: AtomicUsize,
    control: Arc<WorkerControl>,
    abort_handles: Vec<AbortHandle>,
    joins: Mutex<Vec<JoinHandle<()>>>,
    budget: Arc<ForwardBudget>,
    max_lifetime: Duration,
}

const MAX_INFLIGHT_PER_WORKER: usize = 16;
/// Aggregate payload and terminal-fallback owner budget per forwarder.
pub const DEFAULT_FORWARD_BYTE_BUDGET: usize = 64 * 1024 * 1024;
/// Operational lease, including queue wait, response wait and reply handoff.
pub const DEFAULT_FORWARD_MAX_LIFETIME: Duration = Duration::from_secs(300);

/// Bounded local forwarding between one exact pair of transport sessions.
///
/// Cold-bind the destination once with [`Self::bind_destination`]. Job admission
/// then accepts its `Copy` [`ForwardDestination`] receipt and a scoped responder.
/// The first responder binds the return session; a different physical return
/// stream is rejected. Multiple source connections and reconnect successors
/// require separate forwarders. Queued jobs retain numeric reply tokens and
/// borrow their endpoints during dispatch, without cloning transport owners.
///
/// Timed methods establish one absolute deadline at admission. That budget
/// covers worker-queue wait, destination admission/response, and a single
/// nonblocking terminal-reply attempt after expiry. Enqueue success is not
/// remote receipt. No-timeout means no application deadline; every task still
/// has the configured operational lease (five minutes by default). Lease expiry
/// cancels only that ask, never the healthy destination transport.
///
/// Queued work is selected earliest-deadline-first, preserving admission order
/// for equal deadlines. Its fixed-width preallocated deadline index avoids
/// heap operations and waiting-set scans. Replies received from destination
/// transports remain owned by their independently bounded receive pools until
/// handoff or operational expiry.
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
        Self::new_with_limits(
            workers,
            capacity,
            DEFAULT_FORWARD_BYTE_BUDGET,
            DEFAULT_FORWARD_MAX_LIFETIME,
            completion_observer,
        )
    }

    /// Configure bounded forwarding. Workers are clamped to 1..=64 and total
    /// queued admission to 128..=65536, plus 16 in-flight slots. The byte budget
    /// is clamped to 1..=u32::MAX; zero lifetime means immediate expiry.
    pub fn new_with_limits(
        workers: usize,
        capacity: usize,
        byte_budget: usize,
        max_lifetime: Duration,
        completion_observer: Option<Arc<dyn AskForwardObserver>>,
    ) -> Self {
        let workers = workers.clamp(1, 64);
        let capacity = capacity.clamp(128, 65536);
        let byte_budget = byte_budget.clamp(1, u32::MAX as usize);
        let max_inflight = capacity.clamp(1, MAX_INFLIGHT_PER_WORKER);
        let permit_limit = capacity.saturating_add(MAX_INFLIGHT_PER_WORKER);
        let budget = Arc::new(ForwardBudget::new(permit_limit, byte_budget));
        let endpoints = Arc::new(ForwardEndpoints::new(permit_limit));
        let control = Arc::new(WorkerControl {
            closed: AtomicBool::new(false),
            publishers: AtomicUsize::new(0),
            remaining: AtomicUsize::new(workers),
            done: Notify::new(),
            #[cfg(test)]
            test_hooks: {
                let hooks = Mutex::new(None);
                drop(hooks.lock().expect("cold lifecycle hook initialization"));
                hooks
            },
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
            let tx = Arc::new(ForwardQueue::new(
                capacity,
                budget.clone(),
                endpoints.clone(),
            ));
            let rx = ForwardReceiver(tx.clone());
            let worker_observer = completion_observer.clone();
            let worker_control = control.clone();
            let worker_control_for_run = worker_control.clone();
            let mut worker_exit = WorkerExit {
                control: worker_control.clone(),
                drained: false,
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
                worker_exit.drained = result.is_ok();
                drop(worker_exit);
            });
            abort_handles.push(handle.abort_handle());
            joins.push(handle);
            worker_senders.push(tx);
        }

        let inner = Arc::new(AskForwarderInner {
            endpoints,
            workers: worker_senders,
            next_worker: AtomicUsize::new(0),
            control,
            abort_handles,
            joins: Mutex::new(joins),
            budget,
            max_lifetime,
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
        // Close new publishers atomically. Already admitted publishers retain
        // ownership until enqueue (or rejection); workers drain only after the
        // final publisher releases. No hot-path lock or shutdown spin is needed.
        control
            .publishers
            .fetch_or(PUBLISH_CLOSED, Ordering::AcqRel);
        control.closed.store(true, Ordering::Release);
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

    /// Bind the single destination endpoint during connection setup. The exact
    /// physical session is retained once until this forwarder and its workers
    /// are reclaimed. A second binding returns `WriteQueueFull`; reconnects
    /// require a new forwarder, so stale jobs can never target a successor.
    /// No registry, address lookup, transport clone or allocation occurs when
    /// the returned receipt is later used for job admission.
    pub fn bind_destination(&self, destination: RemoteConnection) -> Result<ForwardDestination> {
        let _publication = PublicationGuard::acquire(&self.inner.control)?;
        self.inner.endpoints.bind(destination)
    }

    pub fn try_forward_actor_ask_no_timeout(
        &self,
        destination: ForwardDestination,
        actor_id: u64,
        type_hash: u32,
        payload: ForwardPayload,
        responder: AskResponder<'_>,
    ) -> Result<()> {
        let publication = PublicationGuard::acquire(&self.inner.control)?;
        if !self.inner.endpoints.accepts(destination) {
            return Err(GossipError::Shutdown);
        }
        let session = self.inner.endpoints.reply_session(&responder)?;
        let admission = match responder.prepare_forward_reply(session) {
            Ok(admission) => Some(admission),
            Err(error) if is_duplicate_reply_claim(&error) => None,
            Err(error) => return Err(error),
        };
        self.try_send_task(
            ForwardTask {
                actor_id,
                type_hash,
                payload,
                responder: admission.as_ref().map(ForwardReplyAdmission::token),
                deadline: None,
                timeout_reply: None,
                error_reply: None,
                retained_bytes: 0,
            },
            admission,
            publication,
        )
    }

    /// Forward with a destination timeout. The duration is converted to an
    /// absolute deadline at this call; queue wait, destination wait, and
    /// reply handoff share that budget.
    pub fn try_forward_actor_ask_with_timeout(
        &self,
        destination: ForwardDestination,
        actor_id: u64,
        type_hash: u32,
        payload: ForwardPayload,
        timeout: Duration,
        responder: AskResponder<'_>,
        timeout_reply: ForwardPayload,
        error_reply: ForwardPayload,
    ) -> Result<()> {
        let publication = PublicationGuard::acquire(&self.inner.control)?;
        let deadline = admission_deadline(timeout);
        if !self.inner.endpoints.accepts(destination) {
            return Err(GossipError::Shutdown);
        }
        let session = self.inner.endpoints.reply_session(&responder)?;
        let admission = match responder.prepare_forward_reply(session) {
            Ok(admission) => Some(admission),
            Err(error) if is_duplicate_reply_claim(&error) => None,
            Err(error) => return Err(error),
        };
        self.try_send_task(
            ForwardTask {
                actor_id,
                type_hash,
                payload,
                responder: admission.as_ref().map(ForwardReplyAdmission::token),
                deadline: Some(deadline),
                timeout_reply: Some(timeout_reply),
                error_reply: Some(error_reply),
                retained_bytes: 0,
            },
            admission,
            publication,
        )
    }

    /// Forward with a combined destination+response timeout. Same admission
    /// deadline contract as [`Self::try_forward_actor_ask_with_timeout`].
    pub fn try_forward_actor_ask_combined_timeout(
        &self,
        destination: ForwardDestination,
        actor_id: u64,
        type_hash: u32,
        payload: ForwardPayload,
        timeout: Duration,
        responder: AskResponder<'_>,
        timeout_reply: ForwardPayload,
        error_reply: ForwardPayload,
    ) -> Result<()> {
        let publication = PublicationGuard::acquire(&self.inner.control)?;
        let deadline = admission_deadline(timeout);
        if !self.inner.endpoints.accepts(destination) {
            return Err(GossipError::Shutdown);
        }
        let session = self.inner.endpoints.reply_session(&responder)?;
        let admission = match responder.prepare_forward_reply(session) {
            Ok(admission) => Some(admission),
            Err(error) if is_duplicate_reply_claim(&error) => None,
            Err(error) => return Err(error),
        };
        self.try_send_task(
            ForwardTask {
                actor_id,
                type_hash,
                payload,
                responder: admission.as_ref().map(ForwardReplyAdmission::token),
                deadline: Some(deadline),
                timeout_reply: Some(timeout_reply),
                error_reply: Some(error_reply),
                retained_bytes: 0,
            },
            admission,
            publication,
        )
    }

    fn try_send_task(
        &self,
        mut task: ForwardTask,
        admission: Option<ForwardReplyAdmission<'_>>,
        _publication: PublicationGuard<'_>,
    ) -> Result<()> {
        let retained_bytes = task
            .payload
            .retained_bytes
            .checked_add(
                task.timeout_reply
                    .as_ref()
                    .map_or(0, |reply| reply.retained_bytes),
            )
            .and_then(|bytes| {
                bytes.checked_add(
                    task.error_reply
                        .as_ref()
                        .map_or(0, |reply| reply.retained_bytes),
                )
            })
            .ok_or(GossipError::WriteQueueFull)?;
        // The scoped admission transaction owns unpublished reply cleanup.
        // Only a receiver reconstructs the queued token's cancellation guard.
        debug_assert_eq!(
            task.responder,
            admission.as_ref().map(ForwardReplyAdmission::token)
        );
        let permit = self.inner.budget.try_acquire(retained_bytes)?;
        task.retained_bytes = retained_bytes;
        let lease_deadline = admission_deadline(self.inner.max_lifetime);
        task.deadline = Some(
            task.deadline
                .map_or(lease_deadline, |deadline| deadline.min(lease_deadline)),
        );
        // Count before sending so a worker that runs immediately cannot
        // publish completion before admission is visible to shutdown.
        self.inner.control.admitted.fetch_add(1, Ordering::AcqRel);
        #[cfg(test)]
        if let Some(hook) = lifecycle_test_hook(&self.inner.control, |hooks| {
            hooks.publisher_before_enqueue.clone()
        }) {
            hook.pause();
        }
        let worker_count = self.inner.workers.len();
        let worker_idx = self.inner.next_worker.fetch_add(1, Ordering::Relaxed) % worker_count;
        match self.inner.workers[worker_idx].try_send(task, permit, admission) {
            Ok(()) => Ok(()),
            Err(err) => {
                self.inner.control.admitted.fetch_sub(1, Ordering::AcqRel);
                Err(err)
            }
        }
    }
}

impl Drop for AskForwarderInner {
    fn drop(&mut self) {
        self.control
            .publishers
            .fetch_or(PUBLISH_CLOSED, Ordering::AcqRel);
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
        if control.remaining.load(Ordering::Acquire) == 0
            && control.publishers.load(Ordering::Acquire) == PUBLISH_CLOSED
        {
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
        if control.remaining.load(Ordering::Acquire) == 0
            && control.publishers.load(Ordering::Acquire) == PUBLISH_CLOSED
        {
            return true;
        }
        tokio::select! {
            _ = &mut notified => {}
            _ = &mut sleep => {
                return control.remaining.load(Ordering::Acquire) == 0
                    && control.publishers.load(Ordering::Acquire) == PUBLISH_CLOSED;
            }
        }
    }
}

struct WorkerExit {
    control: Arc<WorkerControl>,
    drained: bool,
}

impl Drop for WorkerExit {
    fn drop(&mut self) {
        // Cancellation, including before the first poll, is not a drained
        // worker just because a publisher subsequently rolls its count back.
        if !self.drained {
            self.control.abnormal.store(true, Ordering::Release);
        }
        let was_last = self.control.remaining.fetch_sub(1, Ordering::AcqRel) == 1;
        if was_last {
            self.control
                .publishers
                .fetch_or(PUBLISH_CLOSED, Ordering::AcqRel);
            self.control.closed.store(true, Ordering::Release);
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
    // An already admitted publisher may still transfer its payload or roll
    // back a rejected send. Reconcile only after that ownership handoff ends.
    if control.publishers.load(Ordering::Acquire) != PUBLISH_CLOSED {
        return false;
    }
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

const PUBLISH_CLOSED: usize = 1 << (usize::BITS - 1);

struct PublicationGuard<'a> {
    control: &'a WorkerControl,
}

impl<'a> PublicationGuard<'a> {
    fn acquire(control: &'a WorkerControl) -> Result<Self> {
        let state = control.publishers.load(Ordering::Acquire);
        if state & PUBLISH_CLOSED != 0 {
            return Err(GossipError::Shutdown);
        }
        if state == PUBLISH_CLOSED - 1 {
            return Err(GossipError::WriteQueueFull);
        }
        // Contention is explicit local backpressure, not a spin loop.
        control
            .publishers
            .compare_exchange(state, state + 1, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|current| {
                if current & PUBLISH_CLOSED != 0 {
                    GossipError::Shutdown
                } else {
                    GossipError::WriteQueueFull
                }
            })?;
        Ok(Self { control })
    }
}

impl Drop for PublicationGuard<'_> {
    fn drop(&mut self) {
        if self.control.publishers.fetch_sub(1, Ordering::AcqRel) == PUBLISH_CLOSED + 1 {
            self.control.shutdown_wake.notify_waiters();
            self.control.shutdown_wake.notify_one();
            if self.control.remaining.load(Ordering::Acquire) == 0 {
                finalize_observer_gaps(self.control);
                self.control.done.notify_waiters();
            }
        }
    }
}

struct ForwardReady {
    bits: AtomicUsize,
    worker: AtomicWaker,
}

struct ForwardSlotWake {
    bit: usize,
    ready: Arc<ForwardReady>,
}

impl Wake for ForwardSlotWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.ready.bits.fetch_or(self.bit, Ordering::Release);
        self.ready.worker.wake();
    }
}

/// Futures live in one fixed allocation. Wakers are constructed once, and
/// readiness is a bitmap rather than an allocated queue node per forwarding
/// operation. Stale wakes can cause a harmless poll, never select old output.
struct ForwardFutureSlab<F> {
    slots: Box<[MaybeUninit<F>]>,
    wakers: Box<[Waker]>,
    ready: Arc<ForwardReady>,
    occupied: usize,
}

impl<F: Future> ForwardFutureSlab<F> {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0 && capacity <= MAX_INFLIGHT_PER_WORKER);
        let ready = Arc::new(ForwardReady {
            bits: AtomicUsize::new(0),
            worker: AtomicWaker::new(),
        });
        let slots = std::iter::repeat_with(MaybeUninit::uninit)
            .take(capacity)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let wakers = (0..capacity)
            .map(|index| {
                Waker::from(Arc::new(ForwardSlotWake {
                    bit: 1 << index,
                    ready: ready.clone(),
                }))
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            slots,
            wakers,
            ready,
            occupied: 0,
        }
    }

    fn len(&self) -> usize {
        self.occupied.count_ones() as usize
    }

    fn is_empty(&self) -> bool {
        self.occupied == 0
    }

    fn push(&mut self, future: F) {
        let index = (!self.occupied).trailing_zeros() as usize;
        assert!(
            index < self.slots.len(),
            "inflight admission owns a free slot"
        );
        self.slots[index].write(future);
        self.occupied |= 1 << index;
        self.ready.bits.fetch_or(1 << index, Ordering::Release);
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<F::Output>> {
        if self.is_empty() {
            return Poll::Ready(None);
        }
        self.ready.worker.register(cx.waker());
        let mut ready = self.ready.bits.swap(0, Ordering::AcqRel) & self.occupied;
        while ready != 0 {
            let index = ready.trailing_zeros() as usize;
            let bit = 1 << index;
            ready &= !bit;
            let mut slot_context = Context::from_waker(&self.wakers[index]);
            // SAFETY: occupied slots are initialized, the boxed allocation
            // never moves, and this exclusive borrow prevents concurrent polls.
            let future = unsafe { Pin::new_unchecked(self.slots[index].assume_init_mut()) };
            if let Poll::Ready(output) = future.poll(&mut slot_context) {
                self.occupied &= !bit;
                // SAFETY: polling has ended, output is owned, and this is the
                // only destruction of the initialized slot before reuse.
                unsafe { self.slots[index].assume_init_drop() };
                self.ready.bits.fetch_or(ready, Ordering::Release);
                return Poll::Ready(Some(output));
            }
        }
        Poll::Pending
    }

    async fn next(&mut self) -> Option<F::Output> {
        poll_fn(|cx| self.poll_next(cx)).await
    }
}

impl<F> Drop for ForwardFutureSlab<F> {
    fn drop(&mut self) {
        while self.occupied != 0 {
            let index = self.occupied.trailing_zeros() as usize;
            self.occupied &= !(1 << index);
            // SAFETY: only occupied slots contain initialized pinned futures;
            // they are dropped in place, including on worker cancellation.
            unsafe { self.slots[index].assume_init_drop() };
        }
    }
}

const NO_DEADLINE_NODE: usize = usize::MAX;
const DEADLINE_LEAF: u8 = 128;

#[derive(Clone)]
struct DeadlineNode {
    key: u128,
    bit: u8,
    child: [usize; 2],
    parent: usize,
    head: usize,
    tail: usize,
}

impl Default for DeadlineNode {
    fn default() -> Self {
        Self {
            key: 0,
            bit: DEADLINE_LEAF,
            child: [NO_DEADLINE_NODE; 2],
            parent: NO_DEADLINE_NODE,
            head: NO_DEADLINE_NODE,
            tail: NO_DEADLINE_NODE,
        }
    }
}

/// Fixed-width Patricia deadline index. Search/insert/remove visit at most
/// 128 key bits regardless of queue length; equal deadlines form an intrusive
/// FIFO. All tree nodes, task slots and free lists are allocated at construction.
struct ForwardDeadlineQueue<T> {
    epoch: Instant,
    nodes: Box<[DeadlineNode]>,
    tasks: Box<[Option<T>]>,
    next: Box<[usize]>,
    free_nodes: Vec<usize>,
    free_tasks: Vec<usize>,
    root: usize,
    len: usize,
}

impl<T> ForwardDeadlineQueue<T> {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            epoch: Instant::now(),
            nodes: vec![DeadlineNode::default(); capacity * 2].into_boxed_slice(),
            tasks: std::iter::repeat_with(|| None)
                .take(capacity)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            next: vec![NO_DEADLINE_NODE; capacity].into_boxed_slice(),
            free_nodes: (0..capacity * 2).rev().collect(),
            free_tasks: (0..capacity).rev().collect(),
            root: NO_DEADLINE_NODE,
            len: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn key(&self, deadline: Option<Instant>) -> u128 {
        deadline.map_or(u128::MAX, |deadline| {
            deadline.saturating_duration_since(self.epoch).as_nanos()
        })
    }

    fn direction(key: u128, bit: u8) -> usize {
        ((key >> (127 - bit)) & 1) as usize
    }

    fn push(&mut self, deadline: Option<Instant>, task: T) {
        let key = self.key(deadline);
        let slot = self
            .free_tasks
            .pop()
            .expect("forward count permit owns a slot");
        self.tasks[slot] = Some(task);
        self.next[slot] = NO_DEADLINE_NODE;
        self.len += 1;
        if self.root == NO_DEADLINE_NODE {
            let leaf = self.free_nodes.pop().expect("preallocated first leaf");
            self.nodes[leaf] = DeadlineNode {
                key,
                head: slot,
                tail: slot,
                ..Default::default()
            };
            self.root = leaf;
            return;
        }
        let mut leaf = self.root;
        while self.nodes[leaf].bit != DEADLINE_LEAF {
            leaf = self.nodes[leaf].child[Self::direction(key, self.nodes[leaf].bit)];
        }
        if self.nodes[leaf].key == key {
            self.next[self.nodes[leaf].tail] = slot;
            self.nodes[leaf].tail = slot;
            return;
        }
        let branch_bit = (self.nodes[leaf].key ^ key).leading_zeros() as u8;
        let mut parent = NO_DEADLINE_NODE;
        let mut subtree = self.root;
        while self.nodes[subtree].bit < branch_bit {
            parent = subtree;
            subtree = self.nodes[subtree].child[Self::direction(key, self.nodes[subtree].bit)];
        }
        let new_leaf = self.free_nodes.pop().expect("preallocated deadline leaf");
        let branch = self.free_nodes.pop().expect("preallocated deadline branch");
        self.nodes[new_leaf] = DeadlineNode {
            key,
            parent: branch,
            head: slot,
            tail: slot,
            ..Default::default()
        };
        let direction = Self::direction(key, branch_bit);
        let mut child = [subtree; 2];
        child[direction] = new_leaf;
        self.nodes[branch] = DeadlineNode {
            bit: branch_bit,
            child,
            parent,
            ..Default::default()
        };
        self.nodes[subtree].parent = branch;
        if parent == NO_DEADLINE_NODE {
            self.root = branch;
        } else {
            let direction = Self::direction(key, self.nodes[parent].bit);
            self.nodes[parent].child[direction] = branch;
        }
    }

    fn first_leaf(&self) -> Option<usize> {
        if self.is_empty() {
            return None;
        }
        let mut node = self.root;
        while self.nodes[node].bit != DEADLINE_LEAF {
            node = self.nodes[node].child[0];
        }
        Some(node)
    }

    fn peek(&self) -> Option<&T> {
        self.first_leaf()
            .and_then(|leaf| self.tasks[self.nodes[leaf].head].as_ref())
    }

    fn pop(&mut self) -> Option<T> {
        let leaf = self.first_leaf()?;
        let slot = self.nodes[leaf].head;
        self.nodes[leaf].head = self.next[slot];
        self.len -= 1;
        let task = self.tasks[slot].take();
        self.free_tasks.push(slot);
        if self.nodes[leaf].head != NO_DEADLINE_NODE {
            return task;
        }
        let parent = self.nodes[leaf].parent;
        self.free_nodes.push(leaf);
        if parent == NO_DEADLINE_NODE {
            self.root = NO_DEADLINE_NODE;
        } else {
            let [left, right] = self.nodes[parent].child;
            let sibling = if left == leaf { right } else { left };
            let grandparent = self.nodes[parent].parent;
            self.nodes[sibling].parent = grandparent;
            if grandparent == NO_DEADLINE_NODE {
                self.root = sibling;
            } else {
                let side = usize::from(self.nodes[grandparent].child[1] == parent);
                self.nodes[grandparent].child[side] = sibling;
            }
            self.free_nodes.push(parent);
        }
        task
    }
}

async fn run_forward_worker(
    rx: ForwardReceiver,
    worker_observer: Option<Arc<dyn AskForwardObserver>>,
    control: Arc<WorkerControl>,
    max_inflight: usize,
) {
    let mut inflight = ForwardFutureSlab::new(max_inflight);
    let mut waiting = ForwardDeadlineQueue::new(rx.0.queue.capacity() + MAX_INFLIGHT_PER_WORKER);
    let mut rx_closed = false;

    loop {
        // Register before checking `closed`: shutdown can race this loop
        // between the check and select. Keeping this same enabled future for
        // the select closes that registration gap for every worker.
        let shutdown_notified = control.shutdown_wake.notified();
        tokio::pin!(shutdown_notified);
        shutdown_notified.as_mut().enable();

        if control.closed.load(Ordering::Acquire)
            && control.publishers.load(Ordering::Acquire) == PUBLISH_CLOSED
            && !rx_closed
        {
            // Closure and the last publisher handoff precede this sweep, so
            // every task whose sender returned Ok(()) is visible. Only after
            // the queue is empty is it safe to close the receiver.
            while let Some(task) = rx.try_recv() {
                waiting.push(task.0.deadline, task);
            }
            rx_closed = true;
        }
        expire_waiting(
            &mut waiting,
            worker_observer.as_deref(),
            &control,
            &rx.0.endpoints,
        );
        while inflight.len() < max_inflight {
            let Some(task) = waiting.pop() else {
                break;
            };
            if task
                .0
                .deadline
                .is_some_and(|deadline| deadline <= Instant::now())
            {
                complete_expired_queued(
                    task,
                    worker_observer.as_deref(),
                    &control,
                    &rx.0.endpoints,
                );
                continue;
            }
            inflight.push(run_forward_task_isolated(task, &rx.0.endpoints));
        }

        #[cfg(test)]
        if !waiting.is_empty()
            && !inflight.is_empty()
            && !rx_closed
            && !rx.0.queue.is_empty()
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
                    Some(task) => waiting.push(task.0.deadline, task),
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

fn earliest_deadline(
    waiting: &ForwardDeadlineQueue<(ForwardTask, ForwardPermit<'_>)>,
) -> Option<Instant> {
    waiting.peek().and_then(|entry| entry.0.deadline)
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
    waiting: &mut ForwardDeadlineQueue<(ForwardTask, ForwardPermit<'_>)>,
    observer: Option<&dyn AskForwardObserver>,
    control: &WorkerControl,
    endpoints: &ForwardEndpoints,
) {
    let now = Instant::now();
    while waiting
        .peek()
        .is_some_and(|entry| entry.0.deadline.is_some_and(|deadline| deadline <= now))
    {
        let entry = waiting.pop().expect("peeked waiting task");
        complete_expired_queued(entry, observer, control, endpoints);
    }
}

fn complete_expired_queued(
    (task, _permit): (ForwardTask, ForwardPermit<'_>),
    observer: Option<&dyn AskForwardObserver>,
    control: &WorkerControl,
    endpoints: &ForwardEndpoints,
) {
    complete_expired_task(task, observer, control, endpoints);
}

fn complete_expired_task(
    task: ForwardTask,
    observer: Option<&dyn AskForwardObserver>,
    control: &WorkerControl,
    endpoints: &ForwardEndpoints,
) {
    let outcome = if let Some(reply) = task.timeout_reply {
        if try_deliver_terminal_reply(endpoints, task.responder, reply) {
            ForwardOutcome::Timeout
        } else {
            ForwardOutcome::ReplyUndeliverable
        }
    } else {
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
async fn run_forward_task_isolated(
    (task, _permit): (ForwardTask, ForwardPermit<'_>),
    endpoints: &ForwardEndpoints,
) -> Option<ForwardOutcome> {
    std::panic::AssertUnwindSafe(run_forward_task(task, endpoints))
        .catch_unwind()
        .await
        .ok()
}

/// Runs the forward and delivers its reply before resolving. Delivery is
/// awaited here, inside the same future the worker's `inflight` set tracks.
/// Timed work uses the admission deadline; expiry never restarts the original
/// duration and reply handoff after the deadline is a single nonblocking try.
async fn run_forward_task(mut task: ForwardTask, endpoints: &ForwardEndpoints) -> ForwardOutcome {
    if let Some(deadline) = task.deadline
        && remaining_until(deadline).is_zero()
    {
        return terminal_timeout(task, endpoints);
    }

    let remaining = task.deadline.map(remaining_until);
    let payload = std::mem::replace(&mut task.payload, ForwardPayload::from_static(&[]));
    let Some(destination) = endpoints.destination.get() else {
        return ForwardOutcome::Error;
    };
    let destination = &destination.connection;
    let actor_id = task.actor_id;
    let type_hash = task.type_hash;
    // Timed forwards use `ask_actor_frame_owned` so one SlotGuard covers
    // identify-gate wait, write-queue admission, and the response wait.
    // Timeout/cancel drops that guard and unregisters the correlation before
    // returning `GossipError::Timeout`.
    let response = match remaining {
        Some(timeout) => {
            destination
                .ask_actor_frame_owned(actor_id, type_hash, payload, timeout)
                .await
        }
        None => {
            destination
                .ask_actor_frame_no_timeout_owned(actor_id, type_hash, payload)
                .await
        }
    };

    match response {
        Ok(reply) => match ForwardPayload::try_from(reply) {
            Ok(reply) => {
                deliver_result_reply(task, reply, ForwardOutcome::Success, endpoints).await
            }
            Err(_) => ForwardOutcome::ReplyUndeliverable,
        },
        Err(GossipError::Timeout) => terminal_timeout(task, endpoints),
        Err(_) => {
            if let Some(reply) = task.error_reply.take() {
                deliver_result_reply(task, reply, ForwardOutcome::Error, endpoints).await
            } else {
                ForwardOutcome::Error
            }
        }
    }
}

fn terminal_timeout(task: ForwardTask, endpoints: &ForwardEndpoints) -> ForwardOutcome {
    if let Some(reply) = task.timeout_reply {
        if try_deliver_terminal_reply(endpoints, task.responder, reply) {
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
    reply: ForwardPayload,
    success: ForwardOutcome,
    endpoints: &ForwardEndpoints,
) -> ForwardOutcome {
    match task.deadline {
        None => {
            if deliver_forwarded_reply_outcome(endpoints, task.responder, reply).await {
                success
            } else {
                ForwardOutcome::ReplyUndeliverable
            }
        }
        Some(deadline) => {
            let remaining = remaining_until(deadline);
            if remaining.is_zero() {
                if try_deliver_terminal_reply(endpoints, task.responder, reply) {
                    success
                } else {
                    ForwardOutcome::ReplyUndeliverable
                }
            } else {
                match tokio::time::timeout(
                    remaining,
                    deliver_forwarded_reply_outcome(endpoints, task.responder, reply),
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

async fn deliver_forwarded_reply_outcome(
    endpoints: &ForwardEndpoints,
    responder: Option<ForwardReplyToken>,
    reply: ForwardPayload,
) -> bool {
    let (Some(session), Some(token)) = (endpoints.reply.get(), responder) else {
        return false;
    };
    match session.reply_owned(token, reply).await {
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

fn try_deliver_terminal_reply(
    endpoints: &ForwardEndpoints,
    responder: Option<ForwardReplyToken>,
    reply: ForwardPayload,
) -> bool {
    let (Some(session), Some(token)) = (endpoints.reply.get(), responder) else {
        return false;
    };
    session.try_reply_owned_immediate(token, reply).is_ok()
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
/// [`AskResponder::reply_owned`] for the bounded ownership-preserving contract.
#[cfg(test)]
async fn deliver_forwarded_reply(responder: AskResponder<'_>, reply: Bytes) {
    let endpoints = ForwardEndpoints::new(1);
    let session = endpoints.reply_session(&responder).unwrap();
    let token = match responder.into_forward_reply(session) {
        Ok(token) => Some(token),
        Err(error) if is_duplicate_reply_claim(&error) => None,
        Err(error) => panic!("test reply endpoint admission failed: {error}"),
    };
    let _ =
        deliver_forwarded_reply_outcome(&endpoints, token, ForwardPayload::copy_from_slice(&reply))
            .await;
}

/// True when `err` is the single-use guard's duplicate-claim rejection
/// (see `ask_responder::claim_reply`) rather than a genuine delivery
/// failure worth logging.
fn is_duplicate_reply_claim(err: &GossipError) -> bool {
    matches!(err, GossipError::Network(e) if e.kind() == std::io::ErrorKind::AlreadyExists)
}

/// Thread-scoped evidence only: cold setup and work on unrelated test threads
/// are excluded. This does not measure allocator internals or cross-thread work.
#[cfg(test)]
pub(crate) mod allocation_evidence {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub(crate) struct Counts {
        pub allocations: usize,
        pub reallocations: usize,
        pub requested_bytes: usize,
    }

    thread_local! {
        static COUNTS: Cell<Option<Counts>> = const { Cell::new(None) };
    }

    struct CountingAllocator;

    fn record(bytes: usize, reallocation: bool) {
        // Constant-initialized, destructor-free TLS and Cell operations do not
        // allocate or recurse into the allocator. Ignore TLS teardown failures.
        let _ = COUNTS.try_with(|cell| {
            if let Some(mut counts) = cell.get() {
                if reallocation {
                    counts.reallocations = counts.reallocations.saturating_add(1);
                } else {
                    counts.allocations = counts.allocations.saturating_add(1);
                }
                counts.requested_bytes = counts.requested_bytes.saturating_add(bytes);
                cell.set(Some(counts));
            }
        });
    }

    // SAFETY: every operation delegates unchanged to System, and accounting
    // neither accesses the allocation nor changes its layout or lifetime.
    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            record(layout.size(), false);
            unsafe { System.alloc(layout) }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            record(layout.size(), false);
            unsafe { System.alloc_zeroed(layout) }
        }

        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            unsafe { System.dealloc(pointer, layout) }
        }

        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            record(size, true);
            unsafe { System.realloc(pointer, layout, size) }
        }
    }

    #[global_allocator]
    static ALLOCATOR: CountingAllocator = CountingAllocator;

    pub(crate) fn measure<T>(work: impl FnOnce() -> T) -> (T, Counts) {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                COUNTS.with(|cell| cell.set(None));
            }
        }
        COUNTS.with(|cell| {
            assert!(cell.get().is_none(), "allocation scopes cannot nest");
            cell.set(Some(Counts::default()));
        });
        let reset = Reset;
        let value = work();
        let counts = COUNTS.with(|cell| cell.get().unwrap());
        drop(reset);
        (value, counts)
    }
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
            match filler.try_reply_bytes(ForwardPayload::from_static(b"filler")) {
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
            match filler.try_reply_bytes(ForwardPayload::from_static(b"filler")) {
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
    fn borrowed_forward_budget_saturates_rolls_back_and_reuses_without_allocating() {
        let budget = ForwardBudget::new(2, 8);
        let (_, counts) = allocation_evidence::measure(|| {
            let first = budget.try_acquire(5).unwrap();
            assert!(matches!(
                budget.try_acquire(4),
                Err(GossipError::WriteQueueFull)
            ));
            assert_eq!(budget.count.load(Ordering::Acquire), 1);
            assert_eq!(budget.bytes.load(Ordering::Acquire), 3);
            let second = budget.try_acquire(3).unwrap();
            assert!(matches!(
                budget.try_acquire(0),
                Err(GossipError::WriteQueueFull)
            ));
            drop(first);
            let successor = budget.try_acquire(5).unwrap();
            drop(second);
            drop(successor);
            for _ in 0..1024 {
                drop(budget.try_acquire(8).unwrap());
            }
        });
        assert_eq!(counts, allocation_evidence::Counts::default());
        assert_eq!(budget.count.load(Ordering::Acquire), 2);
        assert_eq!(budget.bytes.load(Ordering::Acquire), 8);
        let (positive, counts) =
            allocation_evidence::measure(|| std::hint::black_box(vec![0u8; 17]));
        assert!(
            counts.allocations > 0,
            "counter must observe a real allocation"
        );
        assert!(counts.requested_bytes >= positive.len());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn preallocated_forward_queue_saturates_without_job_allocations_or_budget_refcounts() {
        let (destination, source, sink) = test_destination().await;
        let (io, peer) = tokio::io::duplex(4096);
        let (writer, writer_task, _) = LockFreeStreamHandle::new(
            io,
            test_addr(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let writer = Arc::new(writer);
        let forwarder = AskForwarder::new_with_limits(1, 128, 256, Duration::ZERO, None);
        let destination_token = forwarder.bind_destination(destination.clone()).unwrap();
        let warm_context =
            crate::AskContext::from_stream_handle_with_request_id(0, &writer, None, None);
        forwarder
            .inner
            .endpoints
            .reply_session(&warm_context.responder())
            .unwrap();
        let budget_owners = Arc::strong_count(&forwarder.inner.budget);
        let queue_owners = Arc::strong_count(&forwarder.inner.workers[0]);
        let endpoint_owners = Arc::strong_count(&forwarder.inner.endpoints);
        let writer_owners = Arc::strong_count(&writer);
        // Include context/responder construction and public destination/token
        // validation, queue publication, rejection and claim rollback. Only the
        // cold connection and fixed endpoint storage are outside this scope.
        let (_, counts) = allocation_evidence::measure(|| {
            for id in 1..=129 {
                let context =
                    crate::AskContext::from_stream_handle_with_request_id(id, &writer, None, None);
                let result = forwarder.try_forward_actor_ask_no_timeout(
                    destination_token,
                    1,
                    1,
                    ForwardPayload::from_static(b"x"),
                    context.responder(),
                );
                if id <= 128 {
                    assert!(result.is_ok());
                } else {
                    assert!(matches!(result, Err(GossipError::WriteQueueFull)));
                    // A rejected publication must restore the scoped claim.
                    let session = forwarder.inner.endpoints.reply.get().unwrap();
                    let responder = context.responder();
                    drop(responder.prepare_forward_reply(session).unwrap());
                }
                assert_eq!(Arc::strong_count(&writer), writer_owners);
                assert_eq!(
                    Arc::strong_count(&forwarder.inner.endpoints),
                    endpoint_owners
                );
            }
        });
        assert_eq!(counts, allocation_evidence::Counts::default());
        assert_eq!(Arc::strong_count(&forwarder.inner.budget), budget_owners);
        assert_eq!(Arc::strong_count(&forwarder.inner.workers[0]), queue_owners);
        assert_eq!(forwarder.inner.workers[0].queue.len(), 128);
        assert_eq!(forwarder.inner.budget.count.load(Ordering::Acquire), 16);
        assert_eq!(forwarder.inner.budget.bytes.load(Ordering::Acquire), 128);
        tokio::time::timeout(Duration::from_secs(2), async {
            while forwarder.inner.control.completed.load(Ordering::Acquire) != 128 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all accepted jobs must expire and return their budgets");
        assert_eq!(forwarder.inner.budget.count.load(Ordering::Acquire), 144);
        assert_eq!(forwarder.inner.budget.bytes.load(Ordering::Acquire), 256);
        let successor =
            crate::AskContext::from_stream_handle_with_request_id(130, &writer, None, None);
        forwarder
            .try_forward_actor_ask_no_timeout(
                destination_token,
                1,
                1,
                ForwardPayload::from_static(b"x"),
                successor.responder(),
            )
            .unwrap();
        forwarder.shutdown(Duration::from_secs(1)).await.unwrap();
        assert_eq!(
            forwarder.inner.control.completed.load(Ordering::Acquire),
            129
        );
        assert_eq!(forwarder.inner.budget.count.load(Ordering::Acquire), 144);
        assert_eq!(forwarder.inner.budget.bytes.load(Ordering::Acquire), 256);
        drop(peer);
        writer.shutdown();
        writer_task.await.unwrap();
        source.shutdown().await.unwrap();
        sink.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminal_reply_token_uses_reserved_lane_when_normal_lane_is_full() {
        let (io, mut peer) = tokio::io::duplex(64 * 1024);
        let (writer, writer_task, _) = LockFreeStreamHandle::new(
            io,
            test_addr(),
            ChannelId::TellAsk,
            BufferConfig::default().with_write_queue_capacity(128),
            None,
            None,
        );
        let writer = Arc::new(writer);
        let mut filled = 0u32;
        loop {
            let context = crate::AskContext::from_stream_handle_with_request_id(
                1_000 + filled,
                &writer,
                None,
                None,
            );
            match context
                .responder()
                .try_reply_owned(ForwardPayload::from_static(b"normal-filler"))
            {
                Ok(()) => filled += 1,
                Err(GossipError::WriteQueueFull) => break,
                Err(error) => panic!("unexpected normal admission error: {error}"),
            }
            assert!(filled < 4096, "normal queue must reach a declared bound");
        }
        assert!(filled >= 128, "ordinary lane saturation must be nonvacuous");
        let context =
            crate::AskContext::from_stream_handle_with_request_id(77, &writer, None, None);
        let endpoints = ForwardEndpoints::new(1);
        let session = endpoints.reply_session(&context.responder()).unwrap();
        let token = context.responder().into_forward_reply(session).unwrap();
        assert!(try_deliver_terminal_reply(
            &endpoints,
            Some(token),
            ForwardPayload::from_static(b"reserved-terminal"),
        ));
        assert!(
            context
                .responder()
                .try_reply_owned(ForwardPayload::from_static(b"forbidden-duplicate"))
                .is_err()
        );
        let written = read_all_available(&mut peer, 200, 5).await;
        assert!(
            written
                .windows(b"reserved-terminal".len())
                .any(|window| window == b"reserved-terminal")
        );
        assert!(
            !written
                .windows(b"forbidden-duplicate".len())
                .any(|window| window == b"forbidden-duplicate")
        );
        writer.shutdown();
        writer_task.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn close_during_token_publication_reclaims_and_restores_rejected_claim() {
        let (destination, source, sink) = test_destination().await;
        let (io, peer) = tokio::io::duplex(4096);
        let (writer, writer_task, _) = LockFreeStreamHandle::new(
            io,
            test_addr(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let writer = Arc::new(writer);
        let paused = SyncPause::new();
        let hooks = Arc::new(LifecycleTestHooks {
            publisher_before_enqueue: Some(paused.clone()),
            ..Default::default()
        });
        let observer = Arc::new(LifecycleObserver::default());
        let forwarder = AskForwarder::new_with_test_hooks(1, 128, Some(observer.clone()), hooks);
        let destination = forwarder.bind_destination(destination).unwrap();
        let publisher = tokio::spawn({
            let forwarder = forwarder.clone();
            let writer = writer.clone();
            async move {
                let context =
                    crate::AskContext::from_stream_handle_with_request_id(91, &writer, None, None);
                let result = forwarder.try_forward_actor_ask_no_timeout(
                    destination,
                    1,
                    1,
                    ForwardPayload::from_static(b"publish-race"),
                    context.responder(),
                );
                let session = forwarder.inner.endpoints.reply.get().unwrap();
                let responder = context.responder();
                let retry = responder.prepare_forward_reply(session);
                let restored = retry.is_ok();
                drop(retry);
                (result, restored)
            }
        });
        tokio::time::timeout(Duration::from_secs(2), paused.wait_until_entered())
            .await
            .expect("publisher must hold an actual reply token before enqueue");
        let shutdown = tokio::spawn({
            let forwarder = forwarder.clone();
            async move { forwarder.shutdown(Duration::ZERO).await }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !forwarder.inner.workers[0].closed.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("forced shutdown must close the paused publisher's receiver");
        let premature_completion = shutdown.is_finished();
        paused.release();
        let (result, restored) = publisher.await.unwrap();
        assert!(matches!(result, Err(GossipError::Shutdown)));
        assert!(
            restored,
            "rejected publication must restore the original claim"
        );
        assert!(
            !premature_completion,
            "shutdown must wait for the publisher"
        );
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), shutdown)
                .await
                .unwrap()
                .unwrap(),
            Err(GossipError::Timeout)
        ));
        assert_eq!(forwarder.inner.control.admitted.load(Ordering::Acquire), 0);
        assert_eq!(forwarder.inner.control.completed.load(Ordering::Acquire), 0);
        assert_eq!(forwarder.inner.budget.count.load(Ordering::Acquire), 144);
        assert_eq!(observer.success.load(Ordering::Acquire), 0);
        assert_eq!(observer.error.load(Ordering::Acquire), 0);
        drop(peer);
        writer.shutdown();
        writer_task.await.unwrap();
        source.shutdown().await.unwrap();
        sink.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn canceled_forward_receiver_reclaims_every_charge_after_payload_drop_panic() {
        struct DropProbe {
            drops: Arc<AtomicUsize>,
            panic: bool,
        }
        impl AsRef<[u8]> for DropProbe {
            fn as_ref(&self) -> &[u8] {
                b"x"
            }
        }
        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::AcqRel);
                assert!(!self.panic, "isolated queued payload destructor");
            }
        }
        let (destination, source, sink) = test_destination().await;
        let (io, mut peer) = tokio::io::duplex(4096);
        let (writer, writer_task, _) = LockFreeStreamHandle::new(
            io,
            test_addr(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let writer = Arc::new(writer);
        let budget = Arc::new(ForwardBudget::new(2, 2));
        let endpoints = Arc::new(ForwardEndpoints::new(2));
        endpoints.bind(destination).unwrap();
        let queue = Arc::new(ForwardQueue::new(2, budget.clone(), endpoints));
        let receiver = ForwardReceiver(queue.clone());
        let drops = Arc::new(AtomicUsize::new(0));
        let mut stale = None;
        for id in 1..=2 {
            let context =
                crate::AskContext::from_stream_handle_with_request_id(id, &writer, None, None);
            let responder = context.responder();
            let session = queue.endpoints.reply_session(&responder).unwrap();
            let admission = responder.prepare_forward_reply(session).unwrap();
            if id == 1 {
                stale = Some(admission.token());
            }
            let task = ForwardTask {
                actor_id: 1,
                type_hash: 1,
                payload: ForwardPayload::from_test_owner(
                    DropProbe {
                        drops: drops.clone(),
                        panic: id == 1,
                    },
                    1,
                ),
                responder: Some(admission.token()),
                deadline: None,
                timeout_reply: None,
                error_reply: None,
                retained_bytes: 1,
            };
            queue
                .try_send(task, budget.try_acquire(1).unwrap(), Some(admission))
                .unwrap();
        }
        assert_eq!(budget.count.load(Ordering::Acquire), 0);
        assert_eq!(budget.bytes.load(Ordering::Acquire), 0);
        drop(receiver);
        assert_eq!(drops.load(Ordering::Acquire), 2);
        assert!(queue.queue.is_empty());
        assert!(queue.closed.load(Ordering::Acquire));
        assert_eq!(budget.count.load(Ordering::Acquire), 2);
        assert_eq!(budget.bytes.load(Ordering::Acquire), 2);
        let context = crate::AskContext::from_stream_handle_with_request_id(3, &writer, None, None);
        let session = queue.endpoints.reply.get().unwrap();
        let successor = context.responder().into_forward_reply(session).unwrap();
        session.cancel(stale.unwrap());
        session
            .try_reply_owned_immediate(successor, ForwardPayload::from_static(b"successor"))
            .unwrap();
        let mut returned = [0u8; crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN + 9];
        tokio::time::timeout(Duration::from_secs(2), peer.read_exact(&mut returned))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&returned[4..8], &3u32.to_be_bytes());
        assert_eq!(
            &returned[crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN..],
            b"successor"
        );
        drop(queue);
        assert_eq!(drops.load(Ordering::Acquire), 2);
        drop(peer);
        writer.shutdown();
        writer_task.await.unwrap();
        source.shutdown().await.unwrap();
        sink.shutdown().await.unwrap();
    }

    #[test]
    fn native_payload_transfers_preserve_storage_without_owner_allocations() {
        use bytes::Buf;

        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ForwardPayload>();
        assert_send_sync::<crate::ReplyPayload>();
        let mut vector = Vec::with_capacity(4096);
        vector.extend_from_slice(b"vector");
        let vector_pointer = vector.as_ptr();
        let vector_capacity = vector.capacity();
        let (vector, counts) = allocation_evidence::measure(|| {
            crate::ReplyPayload::from(ForwardPayload::from(vector)).into_forward_payload()
        });
        assert_eq!(counts, allocation_evidence::Counts::default());
        assert_eq!(vector.as_ref().as_ptr(), vector_pointer);
        assert_eq!(vector.retained_bytes(), vector_capacity);

        let mut aligned = crate::aligned::AlignedBuffer::with_capacity(1024);
        aligned.resize(32, 7);
        let aligned_pointer = aligned.as_ptr();
        let aligned_capacity = aligned.capacity();
        let (aligned, counts) = allocation_evidence::measure(|| {
            ForwardPayload::try_from(
                crate::AlignedBytes::from_aligned_vec(aligned)
                    .into_range(16..20)
                    .unwrap(),
            )
            .unwrap()
        });
        assert_eq!(counts, allocation_evidence::Counts::default());
        assert_eq!(aligned.as_ref().as_ptr(), aligned_pointer.wrapping_add(16));
        assert_eq!(aligned.retained_bytes(), aligned_capacity);
        assert_eq!(aligned.as_ref(), &[7; 4]);

        let mut pooled = crate::typed::PooledPayload::try_from_pooled_bytes(8, |bytes| {
            bytes.extend_from_slice(b"01234567");
        })
        .unwrap();
        let pooled_pointer = pooled.as_ref().as_ptr();
        let pooled_capacity = pooled.retained_bytes();
        pooled.advance(3);
        let (pooled, counts) = allocation_evidence::measure(|| {
            let mut payload = ForwardPayload::from(pooled);
            for _ in 0..128 {
                payload = crate::ReplyPayload::from(ForwardPayload::from(
                    crate::typed::PooledPayload::from_owned(payload),
                ))
                .into_forward_payload();
            }
            payload
        });
        assert_eq!(counts, allocation_evidence::Counts::default());
        assert_eq!(pooled.as_ref().as_ptr(), pooled_pointer.wrapping_add(3));
        assert_eq!(pooled.retained_bytes(), pooled_capacity);
        assert_eq!(pooled.as_ref(), b"34567");

        #[derive(rkyv::Archive, rkyv::Serialize)]
        struct NativeForwardValue(u64);
        impl crate::typed::WireType for NativeForwardValue {
            const TYPE_HASH: u64 = 0x4657_444f_574e;
            const TYPE_NAME: &'static str = "NativeForwardValue";
        }
        let serialized = crate::typed::encode_typed_pooled(&NativeForwardValue(7)).unwrap();
        let serialized_pointer = serialized.as_ref().as_ptr();
        let serialized_capacity = serialized.retained_bytes();
        let (serialized, counts) = allocation_evidence::measure(|| {
            let forwarded = ForwardPayload::from(serialized);
            ForwardPayload::from(crate::typed::PooledPayload::from_owned(forwarded))
        });
        assert_eq!(counts, allocation_evidence::Counts::default());
        assert_eq!(serialized.as_ref().as_ptr(), serialized_pointer);
        assert_eq!(serialized.retained_bytes(), serialized_capacity);

        let pool = Arc::new(crate::AlignedBytesPool::new(1));
        let received = crate::AlignedBytes::from_pooled_slice(b"received", pool.clone()).unwrap();
        let receive_pointer = received.as_ref().as_ptr();
        let (received, counts) =
            allocation_evidence::measure(|| ForwardPayload::try_from(received).unwrap());
        assert_eq!(counts, allocation_evidence::Counts::default());
        assert_eq!(received.as_ref().as_ptr(), receive_pointer);
        assert!(
            pool.get_buffer(1).is_err(),
            "forwarding must retain the pool checkout"
        );
        drop(received);
        assert!(
            pool.get_buffer(1).is_ok(),
            "final owner drop must return the checkout"
        );

        // Deliberate sharing remains observable, preserving existing fanout
        // semantics without hiding an allocation in ordinary owner transfers.
        assert!(vector.shared_clone().is_err());
        let (shared, counts) = allocation_evidence::measure(|| vector.into_shared());
        assert!(
            counts.allocations > 0,
            "explicit promotion is a positive counter control"
        );
        let alias = shared.shared_clone().unwrap();
        assert_eq!(alias.as_ref().as_ptr(), vector_pointer);
        assert_eq!(alias.retained_bytes(), vector_capacity);
        drop(shared);
        assert_eq!(alias.as_ref(), b"vector");
        let (bytes, counts) = allocation_evidence::measure(|| aligned.into_bytes());
        assert!(
            counts.allocations > 0,
            "compatibility Bytes promotion is explicit"
        );
        assert_eq!(bytes.as_ptr(), aligned_pointer.wrapping_add(16));
    }

    #[test]
    fn forwarding_payload_preserves_known_owner_and_rejects_opaque_views() {
        let pool = Arc::new(crate::AlignedBytesPool::new(1));
        let aligned = crate::AlignedBytes::from_pooled_slice(b"owned", pool.clone()).unwrap();
        let pointer = aligned.as_ref().as_ptr();
        let payload = ForwardPayload::try_from(aligned).unwrap();
        assert_eq!(payload.as_ref().as_ptr(), pointer);
        assert!(payload.retained_bytes >= payload.len());
        assert!(pool.get_buffer(1).is_err());
        drop(payload);
        assert!(pool.get_buffer(1).is_ok());
        let opaque = crate::AlignedBytes::from_bytes(Bytes::new()).unwrap();
        assert!(ForwardPayload::try_from(opaque).is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn forwarding_admission_accounts_retained_bytes_and_lifetime() {
        let (destination, source, sink) = test_destination().await;
        let (io, peer) = tokio::io::duplex(4096);
        let (writer, writer_task, _) = LockFreeStreamHandle::new(
            io,
            test_addr(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let writer = Arc::new(writer);
        let forwarder = AskForwarder::new_with_limits(1, 128, 8, Duration::ZERO, None);
        let destination_token = forwarder.bind_destination(destination.clone()).unwrap();
        let responder = |id| {
            AskResponder::from_stream_handle(id, writer.clone(), Arc::new(AtomicBool::new(false)))
        };
        let mut oversized_owner = Vec::with_capacity(1024);
        oversized_owner.push(1);
        assert!(matches!(
            forwarder.try_forward_actor_ask_no_timeout(
                destination_token,
                1,
                1,
                oversized_owner.into(),
                responder(1),
            ),
            Err(GossipError::WriteQueueFull)
        ));
        assert_eq!(forwarder.inner.budget.bytes.load(Ordering::Acquire), 8);
        let mut oversized_fallback = Vec::with_capacity(1024);
        oversized_fallback.push(2);
        assert!(matches!(
            forwarder.try_forward_actor_ask_with_timeout(
                destination_token,
                1,
                1,
                ForwardPayload::from_static(b""),
                Duration::from_secs(1),
                responder(4),
                oversized_fallback.into(),
                ForwardPayload::from_static(b""),
            ),
            Err(GossipError::WriteQueueFull)
        ));
        assert_eq!(forwarder.inner.budget.bytes.load(Ordering::Acquire), 8);
        forwarder
            .try_forward_actor_ask_no_timeout(
                destination_token,
                1,
                1,
                ForwardPayload::from_static(b"12345678"),
                responder(2),
            )
            .unwrap();
        assert!(matches!(
            forwarder.try_forward_actor_ask_no_timeout(
                destination_token,
                1,
                1,
                ForwardPayload::from_static(b"x"),
                responder(3),
            ),
            Err(GossipError::WriteQueueFull)
        ));
        forwarder.shutdown(Duration::from_secs(1)).await.unwrap();
        assert_eq!(forwarder.inner.budget.bytes.load(Ordering::Acquire), 8);
        assert_eq!(forwarder.inner.control.completed.load(Ordering::Acquire), 1);
        drop(peer);
        writer.shutdown();
        writer_task.await.unwrap();
        source.shutdown().await.unwrap();
        sink.shutdown().await.unwrap();
    }

    #[test]
    fn deadline_index_preserves_order_ties_and_reuses_all_storage() {
        let mut queue = ForwardDeadlineQueue::new(8);
        let epoch = queue.epoch;
        let nodes = queue.nodes.as_ptr();
        let slots = queue.tasks.as_ptr();
        for _ in 0..3 {
            for (offset, value) in [(7, 70), (1, 10), (4, 40), (1, 11), (3, 30)] {
                queue.push(Some(epoch + Duration::from_nanos(offset)), value);
            }
            queue.push(None, 99);
            assert_eq!(queue.peek(), Some(&10));
            let values: Vec<_> = std::iter::from_fn(|| queue.pop()).collect();
            assert_eq!(values, [10, 11, 30, 40, 70, 99]);
            assert!(queue.is_empty());
            assert_eq!(queue.free_tasks.len(), 8);
            assert_eq!(queue.free_nodes.len(), 16);
            assert_eq!(queue.nodes.as_ptr(), nodes);
            assert_eq!(queue.tasks.as_ptr(), slots);
        }
        // Nonvacuity: exercise every distinct bit position, then delete and
        // reuse the branch path with equal-deadline FIFO entries.
        for bit in 0..62 {
            let later = epoch + Duration::from_nanos(1_u64 << bit);
            queue.push(Some(later), 2);
            queue.push(Some(epoch), 1);
            assert_eq!(queue.pop(), Some(1));
            assert_eq!(queue.pop(), Some(2));
        }
    }

    #[test]
    fn pinned_forward_slab_ignores_stale_wakes_and_drops_pending_once() {
        struct ControlledFuture {
            ready: Arc<AtomicBool>,
            polls: Arc<AtomicUsize>,
            drops: Arc<AtomicUsize>,
            value: usize,
        }
        impl Future for ControlledFuture {
            type Output = usize;

            fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<usize> {
                self.polls.fetch_add(1, Ordering::Relaxed);
                if self.ready.load(Ordering::Acquire) {
                    Poll::Ready(self.value)
                } else {
                    Poll::Pending
                }
            }
        }
        impl Drop for ControlledFuture {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::Relaxed);
            }
        }
        let mut slab = ForwardFutureSlab::new(2);
        let storage = slab.slots.as_ptr();
        let ready = Arc::new(AtomicBool::new(false));
        let polls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let make = |value| ControlledFuture {
            ready: ready.clone(),
            polls: polls.clone(),
            drops: drops.clone(),
            value,
        };
        let mut cx = Context::from_waker(Waker::noop());
        slab.push(make(10));
        assert!(slab.poll_next(&mut cx).is_pending());
        assert_eq!(polls.load(Ordering::Relaxed), 1);
        assert!(slab.poll_next(&mut cx).is_pending());
        assert_eq!(
            polls.load(Ordering::Relaxed),
            1,
            "unready slots are not scanned"
        );
        let old_waker = slab.wakers[0].clone();
        ready.store(true, Ordering::Release);
        old_waker.wake_by_ref();
        assert_eq!(slab.poll_next(&mut cx), Poll::Ready(Some(10)));
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        ready.store(false, Ordering::Release);
        slab.push(make(20));
        old_waker.wake_by_ref();
        assert!(slab.poll_next(&mut cx).is_pending());
        assert_eq!(slab.slots.as_ptr(), storage);
        assert_eq!(slab.len(), 1);
        drop(slab);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn pinned_forward_slab_preserves_self_address_across_move_panic_and_reuse() {
        struct AddressFuture {
            address: std::cell::Cell<usize>,
            ready: Arc<AtomicBool>,
            drops: Arc<AtomicUsize>,
            panic_on_ready: bool,
            _pinned: std::marker::PhantomPinned,
        }
        impl Future for AddressFuture {
            type Output = usize;

            fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<usize> {
                let this = self.as_ref().get_ref();
                let address = std::ptr::from_ref(this) as usize;
                if this.address.get() == 0 {
                    this.address.set(address);
                }
                assert_eq!(this.address.get(), address, "pinned future moved");
                if !this.ready.load(Ordering::Acquire) {
                    return Poll::Pending;
                }
                assert!(!this.panic_on_ready, "isolated test panic");
                Poll::Ready(address)
            }
        }
        impl Drop for AddressFuture {
            fn drop(&mut self) {
                if self.address.get() != 0 {
                    assert_eq!(self.address.get(), std::ptr::from_ref(self) as usize);
                }
                self.drops.fetch_add(1, Ordering::Relaxed);
            }
        }
        let ready = Arc::new(AtomicBool::new(false));
        let drops = Arc::new(AtomicUsize::new(0));
        let make = |panic_on_ready| {
            std::panic::AssertUnwindSafe(AddressFuture {
                address: std::cell::Cell::new(0),
                ready: ready.clone(),
                drops: drops.clone(),
                panic_on_ready,
                _pinned: std::marker::PhantomPinned,
            })
            .catch_unwind()
        };
        let mut slab = ForwardFutureSlab::new(1);
        let mut cx = Context::from_waker(Waker::noop());
        slab.push(make(false));
        assert!(slab.poll_next(&mut cx).is_pending());
        // Move the slab owner after pinning its !Unpin future. Only the box
        // handle moves; the future's self-address must remain unchanged.
        let mut moved = Box::new(slab);
        ready.store(true, Ordering::Release);
        moved.wakers[0].wake_by_ref();
        assert!(matches!(moved.poll_next(&mut cx), Poll::Ready(Some(Ok(_)))));
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        moved.push(make(true));
        assert!(matches!(
            moved.poll_next(&mut cx),
            Poll::Ready(Some(Err(_)))
        ));
        assert_eq!(drops.load(Ordering::Relaxed), 2);
        ready.store(false, Ordering::Release);
        moved.push(make(false));
        assert!(moved.poll_next(&mut cx).is_pending());
        drop(moved);
        assert_eq!(drops.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn deadline_index_mixed_operations_match_stable_order() {
        let mut queue = ForwardDeadlineQueue::new(16);
        let epoch = queue.epoch;
        let mut expected = Vec::new();
        for (sequence, offset) in [8, 3, 9, 3, 0, 15, 7, 7, 4, 2, 13, 1]
            .into_iter()
            .enumerate()
        {
            let deadline = Some(epoch + Duration::from_nanos(offset));
            queue.push(deadline, (offset, sequence));
            expected.push((offset, sequence));
            if (sequence + 1) % 3 == 0 {
                expected.sort();
                assert_eq!(queue.pop(), Some(expected.remove(0)));
            }
        }
        expected.sort();
        for value in expected {
            assert_eq!(queue.pop(), Some(value));
        }
        assert_eq!(queue.pop(), None);
        assert_eq!(queue.free_tasks.len(), 16);
        assert_eq!(queue.free_nodes.len(), 32);
    }

    #[tokio::test]
    async fn publication_close_waits_for_exact_publisher_without_a_lock() {
        let forwarder = AskForwarder::new(1, 128);
        let control = &forwarder.inner.control;
        let publisher = PublicationGuard::acquire(control).unwrap();
        control
            .publishers
            .fetch_or(PUBLISH_CLOSED, Ordering::AcqRel);
        control.closed.store(true, Ordering::Release);
        assert_eq!(
            control.publishers.load(Ordering::Acquire),
            PUBLISH_CLOSED + 1
        );
        assert!(matches!(
            PublicationGuard::acquire(control),
            Err(GossipError::Shutdown)
        ));
        assert_ne!(control.remaining.load(Ordering::Acquire), 0);
        drop(publisher);
        assert_eq!(control.publishers.load(Ordering::Acquire), PUBLISH_CLOSED);
        forwarder.shutdown(Duration::from_secs(1)).await.unwrap();
        assert_eq!(control.remaining.load(Ordering::Acquire), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn forced_shutdown_waits_for_publisher_rollback_before_reconciliation() {
        let observer = Arc::new(LifecycleObserver::default());
        let forwarder = AskForwarder::new_with_observer(1, 128, Some(observer.clone()));
        let control = &forwarder.inner.control;
        let publisher = PublicationGuard::acquire(control).unwrap();
        // Exact mid-publication state: admission is counted before queue push,
        // which may still fail and roll back if its worker is aborted.
        control.admitted.fetch_add(1, Ordering::AcqRel);
        for worker in &forwarder.inner.abort_handles {
            worker.abort();
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while control.remaining.load(Ordering::Acquire) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(observer.error.load(Ordering::Acquire), 0);
        assert!(!control.reconciliation.lock().unwrap().complete);
        let shutdown = forwarder.shutdown(Duration::ZERO);
        tokio::pin!(shutdown);
        assert!(shutdown.as_mut().now_or_never().is_none());
        control.admitted.fetch_sub(1, Ordering::AcqRel);
        drop(publisher);
        assert!(matches!(shutdown.await, Err(GossipError::Timeout)));
        assert_eq!(control.admitted.load(Ordering::Acquire), 0);
        assert_eq!(control.completed.load(Ordering::Acquire), 0);
        assert_eq!(observer.error.load(Ordering::Acquire), 0);
        assert!(control.reconciliation.lock().unwrap().complete);
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
        let destination_token = forwarder.bind_destination(destination.clone()).unwrap();
        forwarder
            .try_forward_actor_ask_no_timeout(
                destination_token,
                1,
                1,
                crate::ask_forwarder::ForwardPayload::from_static(b"post-abort"),
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
        assert_eq!(forwarder.inner.budget.count.load(Ordering::Acquire), 144);

        drop(peer);
        writer.shutdown();
        tokio::time::timeout(Duration::from_secs(2), writer_task)
            .await
            .expect("writer task must be reclaimed")
            .expect("writer task must not panic");
        source.shutdown().await.unwrap();
        sink.shutdown().await.unwrap();
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
        let destination_token = forwarder.bind_destination(destination.clone()).unwrap();
        forwarder
            .try_forward_actor_ask_no_timeout(
                destination_token,
                1,
                1,
                crate::ask_forwarder::ForwardPayload::from_static(b"reconcile-once"),
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
        source.shutdown().await.unwrap();
        sink.shutdown().await.unwrap();
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
        let destination_token = forwarder.bind_destination(destination.clone()).unwrap();
        const ADMITTED: usize = 20;
        for index in 0..ADMITTED {
            forwarder
                .try_forward_actor_ask_no_timeout(
                    destination_token,
                    1,
                    1,
                    crate::ask_forwarder::ForwardPayload::from_static(b"ownership"),
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
        assert_eq!(forwarder.inner.budget.count.load(Ordering::Acquire), 144);

        drop(peer);
        writer.shutdown();
        tokio::time::timeout(Duration::from_secs(2), writer_task)
            .await
            .expect("writer task must be reclaimed")
            .expect("writer task must not panic");
        source.shutdown().await.unwrap();
        sink.shutdown().await.unwrap();
    }

    struct ForwardEcho(Arc<AtomicUsize>);

    impl crate::registry::ActorMessageHandler for ForwardEcho {
        fn handle_actor_message(
            &self,
            _actor_id: u64,
            _type_hash: u32,
            payload: crate::AlignedBytes,
            _correlation_id: Option<u32>,
        ) -> crate::registry::ActorMessageFuture<'_> {
            self.0.fetch_add(1, Ordering::AcqRel);
            Box::pin(async move { Ok(Some(crate::registry::ActorResponse::Aligned(payload))) })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn forwarding_endpoint_tokens_reject_foreign_and_retired_sessions() {
        let (destination, source, sink) = test_destination().await;
        let requests = Arc::new(AtomicUsize::new(0));
        sink.registry
            .set_actor_message_handler(Arc::new(ForwardEcho(requests.clone())))
            .await;
        let (io, mut peer) = tokio::io::duplex(4096);
        let (writer, writer_task, _) = LockFreeStreamHandle::new(
            io,
            test_addr(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let writer = Arc::new(writer);
        let forwarder = AskForwarder::new(1, 128);
        let original_address = destination.addr;
        let original_session = destination.instance_id();
        let token = forwarder.bind_destination(destination.clone()).unwrap();
        assert!(matches!(
            forwarder.bind_destination(destination.clone()),
            Err(GossipError::WriteQueueFull)
        ));
        let foreign = AskForwarder::new(1, 128);
        let foreign_token = foreign.bind_destination(destination).unwrap();
        let context =
            crate::AskContext::from_stream_handle_with_request_id(101, &writer, None, None);
        assert!(matches!(
            forwarder.try_forward_actor_ask_no_timeout(
                foreign_token,
                1,
                1,
                ForwardPayload::from_static(b"forbidden"),
                context.responder(),
            ),
            Err(GossipError::Shutdown)
        ));
        assert_eq!(forwarder.inner.control.admitted.load(Ordering::Acquire), 0);
        forwarder
            .try_forward_actor_ask_with_timeout(
                token,
                1,
                1,
                ForwardPayload::from_static(b"live"),
                Duration::from_secs(1),
                context.responder(),
                ForwardPayload::from_static(b"unexpected-timeout"),
                ForwardPayload::from_static(b"unexpected-error"),
            )
            .unwrap();
        let mut frame = [0u8; crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN + 4];
        tokio::time::timeout(Duration::from_secs(2), peer.read_exact(&mut frame))
            .await
            .expect("accepted endpoint token must complete the full transport roundtrip")
            .unwrap();
        assert_eq!(
            &frame[..crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN],
            &crate::framing::write_ask_response_header(crate::MessageType::Response, 101, 4)
        );
        assert_eq!(
            &frame[crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN..],
            b"live"
        );
        assert_eq!(requests.load(Ordering::Acquire), 1);
        let (foreign_io, mut foreign_peer) = tokio::io::duplex(4096);
        let (foreign_writer, foreign_writer_task, _) = LockFreeStreamHandle::new(
            foreign_io,
            test_addr(),
            ChannelId::TellAsk,
            BufferConfig::default(),
            None,
            None,
        );
        let foreign_writer = Arc::new(foreign_writer);
        let foreign_context =
            crate::AskContext::from_stream_handle_with_request_id(103, &foreign_writer, None, None);
        assert!(matches!(
            forwarder.try_forward_actor_ask_no_timeout(
                token,
                1,
                1,
                ForwardPayload::from_static(b"foreign-source"),
                foreign_context.responder(),
            ),
            Err(GossipError::InvalidConfig(_))
        ));
        assert_eq!(forwarder.inner.control.admitted.load(Ordering::Acquire), 1);
        foreign_context
            .responder()
            .try_reply_owned(ForwardPayload::from_static(b"foreign-alive"))
            .unwrap();
        let mut foreign_reply = [0u8; crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN + 13];
        tokio::time::timeout(
            Duration::from_secs(2),
            foreign_peer.read_exact(&mut foreign_reply),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&foreign_reply[4..8], &103u32.to_be_bytes());
        assert_eq!(
            &foreign_reply[crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN..],
            b"foreign-alive"
        );
        assert_eq!(requests.load(Ordering::Acquire), 1);
        foreign_writer.shutdown();
        foreign_writer_task.await.unwrap();
        forwarder.shutdown(Duration::from_secs(1)).await.unwrap();
        foreign.shutdown(Duration::from_secs(1)).await.unwrap();
        source.shutdown().await.unwrap();
        sink.shutdown().await.unwrap();

        let (mut successor, successor_source, successor_sink) = test_destination().await;
        assert_ne!(successor.instance_id(), original_session);
        // Public address aliases cannot turn an old physical-session receipt
        // into authority over a successor connection.
        successor.addr = original_address;
        let successor_requests = Arc::new(AtomicUsize::new(0));
        successor_sink
            .registry
            .set_actor_message_handler(Arc::new(ForwardEcho(successor_requests.clone())))
            .await;
        let successor_forwarder = AskForwarder::new(1, 128);
        let successor_token = successor_forwarder.bind_destination(successor).unwrap();
        let context =
            crate::AskContext::from_stream_handle_with_request_id(102, &writer, None, None);
        assert!(matches!(
            successor_forwarder.try_forward_actor_ask_no_timeout(
                token,
                1,
                1,
                ForwardPayload::from_static(b"stale"),
                context.responder(),
            ),
            Err(GossipError::Shutdown)
        ));
        successor_forwarder
            .try_forward_actor_ask_with_timeout(
                successor_token,
                1,
                1,
                ForwardPayload::from_static(b"next"),
                Duration::from_secs(1),
                context.responder(),
                ForwardPayload::from_static(b"unexpected-timeout"),
                ForwardPayload::from_static(b"unexpected-error"),
            )
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), peer.read_exact(&mut frame))
            .await
            .expect("successor's own token must remain usable after stale rejection")
            .unwrap();
        assert_eq!(&frame[4..8], &102u32.to_be_bytes());
        assert_eq!(
            &frame[crate::framing::ASK_RESPONSE_FRAME_HEADER_LEN..],
            b"next"
        );
        assert_eq!(successor_requests.load(Ordering::Acquire), 1);
        successor_forwarder
            .shutdown(Duration::from_secs(1))
            .await
            .unwrap();
        writer.shutdown();
        writer_task.await.unwrap();
        successor_source.shutdown().await.unwrap();
        successor_sink.shutdown().await.unwrap();
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
        let destination_token = forwarder.bind_destination(destination.clone()).unwrap();
        let responder =
            AskResponder::from_stream_handle(10, writer.clone(), Arc::new(AtomicBool::new(false)));

        // The worker has not been polled yet, so this accepted task is still
        // in its channel when shutdown closes admission. Shutdown must receive
        // it, complete its expired terminal reply, and account for it.
        forwarder
            .try_forward_actor_ask_combined_timeout(
                destination_token,
                1,
                1,
                crate::ask_forwarder::ForwardPayload::from_static(b"request"),
                Duration::ZERO,
                responder,
                crate::ask_forwarder::ForwardPayload::from_static(b"drained-timeout-reply"),
                crate::ask_forwarder::ForwardPayload::from_static(b"error"),
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
        source.shutdown().await.unwrap();
        sink.shutdown().await.unwrap();
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
        let destination_token = forwarder.bind_destination(destination.clone()).unwrap();
        forwarder
            .try_forward_actor_ask_no_timeout(
                destination_token,
                1,
                1,
                crate::ask_forwarder::ForwardPayload::from_static(b"request"),
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
        source.shutdown().await.unwrap();
        sink.shutdown().await.unwrap();
    }
}
