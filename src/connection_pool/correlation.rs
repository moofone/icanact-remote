/// Maximum number of pending responses (must be power of 2 for fast modulo)
const PENDING_RESPONSES_SIZE: usize = 8192;
const PENDING_RESPONSES_MASK: usize = PENDING_RESPONSES_SIZE - 1;
const SLOT_EMPTY: u8 = 0;
const SLOT_WAITING: u8 = 1;
const SLOT_WRITING: u8 = 2;
const SLOT_READY: u8 = 3;

/// Pending response slot
struct PendingResponseSlot {
    state: AtomicU8,
    response: UnsafeCell<MaybeUninit<crate::AlignedBytes>>,
    waker: AtomicWaker,
}

// Safety: access is synchronized via atomics and the correlation protocol.
unsafe impl Send for PendingResponseSlot {}
unsafe impl Sync for PendingResponseSlot {}

/// Shared state for correlation tracking
pub(crate) struct CorrelationTracker {
    /// Next correlation ID to use
    next_id: AtomicU16,
    /// Fixed-size pending responses (boxed to avoid large stack allocations)
    pending: Box<[PendingResponseSlot]>,
}

impl std::fmt::Debug for CorrelationTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CorrelationTracker")
            .field("next_id", &self.next_id.load(Ordering::Relaxed))
            .finish()
    }
}

impl CorrelationTracker {
    #[inline]
    fn slot_index(correlation_id: u16) -> usize {
        (correlation_id as usize) & PENDING_RESPONSES_MASK
    }

    #[inline]
    fn try_take_ready(slot_ref: &PendingResponseSlot) -> Option<crate::AlignedBytes> {
        let state = slot_ref.state.load(Ordering::Acquire);
        if state == SLOT_READY {
            slot_ref.state.store(SLOT_EMPTY, Ordering::Release);
            let response = unsafe { (*slot_ref.response.get()).assume_init_read() };
            return Some(response);
        }
        None
    }

    fn new() -> Arc<Self> {
        debug_assert!(
            PENDING_RESPONSES_SIZE.is_power_of_two(),
            "PENDING_RESPONSES_SIZE must be power of two"
        );
        let mut pending = Vec::with_capacity(PENDING_RESPONSES_SIZE);
        pending.resize_with(PENDING_RESPONSES_SIZE, || PendingResponseSlot {
            state: AtomicU8::new(SLOT_EMPTY),
            response: UnsafeCell::new(MaybeUninit::uninit()),
            waker: AtomicWaker::new(),
        });
        Arc::new(Self {
            next_id: AtomicU16::new(1),
            pending: pending.into_boxed_slice(),
        })
    }

    /// Allocate a correlation ID and reserve the response slot.
    ///
    /// Returns a borrowed RAII [`SlotGuard`] which cancels the slot on drop
    /// unless [`SlotGuard::disarm`] is called. This is the cancellation-safe
    /// path: async callers can use `?` and rely on the guard to release the
    /// slot if the awaiter is dropped (e.g. by an outer
    /// `tokio::time::timeout` firing).
    ///
    /// Returns [`Err(NoFreeSlots)`](NoFreeSlots) when the entire ring is in
    /// a non-EMPTY state. Previously this method was an unbounded `loop {}`
    /// which monopolised single-threaded tokio runtimes when slots leaked
    /// (production incident raft-1 2026-05-09).
    ///
    /// The returned guard borrows from `self` rather than holding an Arc
    /// clone, so the success path adds zero atomic refcount traffic over
    /// the previous bare-`u16` API.
    pub(crate) fn allocate(&self) -> std::result::Result<SlotGuard<'_>, NoFreeSlots> {
        // Bounded sweep: at most one full pass over the ring. On the
        // overwhelmingly common uncontended hot path the loop exits on the
        // first iteration, so the bound adds one register-resident counter
        // dec+jne per iteration — under one cycle on modern x86, lost in
        // the noise of the existing fetch_add+CAS.
        for _ in 0..PENDING_RESPONSES_SIZE {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            if id == 0 {
                continue; // Skip 0 as it's reserved
            }

            let slot = Self::slot_index(id);
            let slot_ref = &self.pending[slot];
            if slot_ref
                .state
                .compare_exchange(
                    SLOT_EMPTY,
                    SLOT_WAITING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                #[cfg(feature = "trace-correlation")]
                trace!(
                    "CorrelationTracker: Allocated correlation_id {} in slot {}",
                    id, slot
                );
                return Ok(SlotGuard { tracker: self, id });
            }

            // Slot is occupied, try next ID. Gated behind the
            // `trace-correlation` feature: in production this fires once
            // per contended iteration and burned measurable cycles on the
            // tracing dispatcher's filter check even when the level was
            // disabled.
            #[cfg(feature = "trace-correlation")]
            trace!("CorrelationTracker: Slot {} occupied, trying next ID", slot);
        }
        Err(NoFreeSlots)
    }

    /// Complete a pending request with a response.
    ///
    /// Returns true when the response was consumed and published.
    pub(crate) fn complete(
        &self,
        correlation_id: u16,
        response: &mut Option<crate::AlignedBytes>,
    ) -> bool {
        let slot = Self::slot_index(correlation_id);
        let slot_ref = &self.pending[slot];
        if slot_ref
            .state
            .compare_exchange(
                SLOT_WAITING,
                SLOT_WRITING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return false;
        }

        let Some(response) = response.take() else {
            slot_ref.state.store(SLOT_EMPTY, Ordering::Release);
            slot_ref.waker.wake();
            return false;
        };

        // Store response, then publish READY
        unsafe {
            (*slot_ref.response.get()).write(response);
        }
        slot_ref.state.store(SLOT_READY, Ordering::Release);
        slot_ref.waker.wake();
        true
    }

    /// Cancel a pending request (used when send fails).
    pub(crate) fn cancel(&self, correlation_id: u16) {
        let slot = Self::slot_index(correlation_id);
        let slot_ref = &self.pending[slot];
        loop {
            let state = slot_ref.state.load(Ordering::Acquire);
            match state {
                SLOT_WAITING => {
                    if slot_ref
                        .state
                        .compare_exchange(
                            SLOT_WAITING,
                            SLOT_EMPTY,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
                SLOT_READY => {
                    if slot_ref
                        .state
                        .compare_exchange(
                            SLOT_READY,
                            SLOT_EMPTY,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        unsafe {
                            (*slot_ref.response.get()).assume_init_drop();
                        }
                        break;
                    }
                }
                SLOT_WRITING => {
                    std::hint::spin_loop();
                }
                _ => break,
            }
        }
        slot_ref.waker.wake();
    }

    /// Cancel all pending requests (used when a connection drops).
    pub(crate) fn cancel_all(&self) {
        for slot_ref in self.pending.iter() {
            loop {
                let state = slot_ref.state.load(Ordering::Acquire);
                match state {
                    SLOT_WAITING => {
                        if slot_ref
                            .state
                            .compare_exchange(
                                SLOT_WAITING,
                                SLOT_EMPTY,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            slot_ref.waker.wake();
                            break;
                        }
                    }
                    SLOT_READY => {
                        if slot_ref
                            .state
                            .compare_exchange(
                                SLOT_READY,
                                SLOT_EMPTY,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            unsafe {
                                (*slot_ref.response.get()).assume_init_drop();
                            }
                            slot_ref.waker.wake();
                            break;
                        }
                    }
                    SLOT_WRITING => {
                        std::hint::spin_loop();
                    }
                    _ => break,
                }
            }
        }
    }

    async fn wait_for_response(
        &self,
        correlation_id: u16,
        timeout: Duration,
    ) -> Result<crate::AlignedBytes> {
        if timeout.is_zero() {
            return self.wait_for_response_no_timeout(correlation_id).await;
        }
        let slot = Self::slot_index(correlation_id);
        let slot_ref = &self.pending[slot];

        let wait_fut = futures::future::poll_fn(|cx| {
            // If the slot was cancelled (e.g. connection dropped and cancel_all() ran),
            // return a concrete error instead of waiting forever.
            if let Some(response) = Self::try_take_ready(slot_ref) {
                return std::task::Poll::Ready(Ok(response));
            }
            let state = slot_ref.state.load(Ordering::Acquire);
            if state == SLOT_EMPTY {
                return std::task::Poll::Ready(Err(crate::GossipError::ConnectionDropped));
            }

            slot_ref.waker.register(cx.waker());

            if let Some(response) = Self::try_take_ready(slot_ref) {
                return std::task::Poll::Ready(Ok(response));
            }
            let state = slot_ref.state.load(Ordering::Acquire);
            if state == SLOT_EMPTY {
                return std::task::Poll::Ready(Err(crate::GossipError::ConnectionDropped));
            }

            std::task::Poll::Pending
        });

        match tokio::time::timeout(timeout, wait_fut).await {
            Ok(result) => result,
            Err(_) => {
                if slot_ref
                    .state
                    .compare_exchange(
                        SLOT_WAITING,
                        SLOT_EMPTY,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    return Err(crate::GossipError::Timeout);
                }
                if let Some(response) = Self::try_take_ready(slot_ref) {
                    return Ok(response);
                }
                match slot_ref.state.load(Ordering::Acquire) {
                    SLOT_WRITING => self.wait_for_response_no_timeout(correlation_id).await,
                    SLOT_EMPTY => Err(crate::GossipError::ConnectionDropped),
                    _ => Err(crate::GossipError::Timeout),
                }
            }
        }
    }

    async fn wait_for_response_no_timeout(
        &self,
        correlation_id: u16,
    ) -> Result<crate::AlignedBytes> {
        let slot = Self::slot_index(correlation_id);
        let slot_ref = &self.pending[slot];

        futures::future::poll_fn(|cx| {
            if let Some(response) = Self::try_take_ready(slot_ref) {
                return std::task::Poll::Ready(Ok(response));
            }
            let state = slot_ref.state.load(Ordering::Acquire);
            if state == SLOT_EMPTY {
                return std::task::Poll::Ready(Err(crate::GossipError::ConnectionDropped));
            }
            slot_ref.waker.register(cx.waker());
            if let Some(response) = Self::try_take_ready(slot_ref) {
                return std::task::Poll::Ready(Ok(response));
            }
            let state = slot_ref.state.load(Ordering::Acquire);
            if state == SLOT_EMPTY {
                return std::task::Poll::Ready(Err(crate::GossipError::ConnectionDropped));
            }
            std::task::Poll::Pending
        })
        .await
    }
}

/// Returned by [`CorrelationTracker::allocate`] when a full sweep over the
/// 8192-slot ring found no slot in `SLOT_EMPTY` state. Operators seeing
/// this in logs should treat it as evidence of an upstream slot leak —
/// the previous `loop {}` implementation silently spun the executor
/// instead of surfacing the condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NoFreeSlots;

impl std::fmt::Display for NoFreeSlots {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "correlation tracker exhausted: all {PENDING_RESPONSES_SIZE} slots in use"
        )
    }
}

impl std::error::Error for NoFreeSlots {}

/// RAII guard for a correlation slot reservation.
///
/// Holds a shared borrow of the tracker (no Arc clone) and a single 16-bit
/// id. On drop the slot is cancelled — this is what closes the production
/// leak where a future awaiting [`CorrelationTracker::wait_for_response`]
/// could be cancelled mid-await (outer `tokio::time::timeout`, `select!`
/// arm losing) without restoring slot state.
///
/// Call [`SlotGuard::disarm`] on the success path to consume the guard
/// without running the cancellation Drop. The disarm path uses
/// `mem::forget`, so the success path adds zero atomic ops over the
/// previous bare-`u16` API.
#[must_use = "dropping a SlotGuard cancels the slot; call .disarm() on success"]
pub(crate) struct SlotGuard<'a> {
    tracker: &'a CorrelationTracker,
    id: u16,
}

impl<'a> SlotGuard<'a> {
    #[inline(always)]
    pub(crate) fn id(&self) -> u16 {
        self.id
    }

    /// Consume the guard without running the cancellation Drop. Use this
    /// after the consumer has moved the slot out of `SLOT_WAITING`
    /// (i.e. on the success path of `wait_for_response`).
    #[inline(always)]
    pub(crate) fn disarm(self) -> u16 {
        let id = self.id;
        std::mem::forget(self);
        id
    }
}

impl Drop for SlotGuard<'_> {
    // `#[cold]` + `#[inline(never)]` keep the cancel path out of the hot
    // icache — Drop only runs on the cancellation path, never on success.
    #[cold]
    #[inline(never)]
    fn drop(&mut self) {
        self.tracker.cancel(self.id);
    }
}

impl std::fmt::Debug for SlotGuard<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlotGuard").field("id", &self.id).finish()
    }
}
