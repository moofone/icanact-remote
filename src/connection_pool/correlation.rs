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
    fn allocate(&self) -> u16 {
        loop {
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
                trace!(
                    "CorrelationTracker: Allocated correlation_id {} in slot {}",
                    id, slot
                );
                return id;
            }

            // Slot is occupied, try next ID (trace level - fires frequently under load)
            trace!("CorrelationTracker: Slot {} occupied, trying next ID", slot);
        }
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
                return std::task::Poll::Ready(Err(crate::GossipError::Timeout));
            }

            slot_ref.waker.register(cx.waker());

            if let Some(response) = Self::try_take_ready(slot_ref) {
                return std::task::Poll::Ready(Ok(response));
            }
            let state = slot_ref.state.load(Ordering::Acquire);
            if state == SLOT_EMPTY {
                return std::task::Poll::Ready(Err(crate::GossipError::Timeout));
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
                // The slot isn't WAITING anymore. It might be READY/WRITING/EMPTY (cancelled).
                // READY: return the response.
                // WRITING: wait without timeout to avoid dropping an in-progress write.
                // EMPTY: treat as timeout/cancel to avoid hanging forever.
                if let Some(response) = Self::try_take_ready(slot_ref) {
                    return Ok(response);
                }
                match slot_ref.state.load(Ordering::Acquire) {
                    SLOT_WRITING => self.wait_for_response_no_timeout(correlation_id).await,
                    SLOT_EMPTY => Err(crate::GossipError::Timeout),
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
                return std::task::Poll::Ready(Err(crate::GossipError::Timeout));
            }
            slot_ref.waker.register(cx.waker());
            if let Some(response) = Self::try_take_ready(slot_ref) {
                return std::task::Poll::Ready(Ok(response));
            }
            let state = slot_ref.state.load(Ordering::Acquire);
            if state == SLOT_EMPTY {
                return std::task::Poll::Ready(Err(crate::GossipError::Timeout));
            }
            std::task::Poll::Pending
        })
        .await
    }
}
