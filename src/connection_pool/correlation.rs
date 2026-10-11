/// Maximum number of pending responses (must be power of 2 for fast modulo)
const PENDING_RESPONSES_SIZE: usize = 8192;
const PENDING_RESPONSES_MASK: usize = PENDING_RESPONSES_SIZE - 1;
const SLOT_EMPTY: u64 = 0;
const SLOT_WAITING: u64 = 1;
const SLOT_WRITING: u64 = 2;
const SLOT_READY: u64 = 3;
const SLOT_CANCEL_REQUESTED: u64 = 1 << 63;
/// Per-attempt work budget; collisions return typed admission backpressure.
const CORRELATION_ADMISSION_PROBES: usize = 32;

/// What a completed slot resolves to: a real payload, or an explicit NACK
/// from the peer. `Nack` is `Copy` (a tag + a one-byte reason), so publishing
/// it costs nothing beyond what `Response` already pays to publish a payload.
pub(crate) enum CorrelationOutcome {
    Response(crate::AlignedBytes),
    /// A real payload completed by a connection whose read context carried a
    /// certificate-authenticated identity. The identity is published in the
    /// same slot write as the payload, so the two can never be torn apart.
    /// (`Arc` keeps the slot ring small: the identity is shared with the
    /// completing connection's `ReadContext`, not copied per reply.)
    Attributed(crate::AlignedBytes, Arc<crate::PeerId>),
    Nack(crate::framing::AskNackReason),
}

impl CorrelationOutcome {
    /// Convert to the `Result` a waiter actually receives: a NACK becomes an
    /// immediate typed error rather than a payload the caller must inspect.
    pub(crate) fn into_result(self) -> Result<crate::AlignedBytes> {
        match self {
            Self::Response(bytes) | Self::Attributed(bytes, _) => Ok(bytes),
            Self::Nack(reason) => Err(crate::GossipError::AskNacked(reason)),
        }
    }

    /// Like [`Self::into_result`], but also yields the authenticated identity
    /// of the connection that completed the reply. A reply completed without
    /// an authenticated identity is an error, never a placeholder.
    pub(crate) fn into_attributed_result(self) -> Result<(crate::PeerId, crate::AlignedBytes)> {
        match self {
            Self::Attributed(bytes, peer) => Ok(((*peer).clone(), bytes)),
            Self::Response(_) => Err(crate::GossipError::AuthenticationFailed(
                "reply was not completed by a connection with an authenticated peer identity"
                    .to_string(),
            )),
            Self::Nack(reason) => Err(crate::GossipError::AskNacked(reason)),
        }
    }
}

/// Outcome of attempting to take a slot's ready response.
enum ReadyTake {
    /// Slot was not READY.
    NotReady,
    /// Took this waiter's own response.
    Taken(CorrelationOutcome),
    /// Slot is READY but belongs to a different correlation id: this waiter's
    /// request was recycled (R-10). The caller must stop waiting.
    ForeignReady,
}

/// Pending response slot
struct PendingResponseSlot {
    /// REMOTE-3/4: one atomic word owns both the full wire ID and state.
    /// Stale operations must never acquire a successor's transient WRITING
    /// state, even if they would later restore it after detecting a mismatch.
    /// AtomicU64 is already required by the connection pool's generation and
    /// byte counters. This does not extend the u32 wire-ID exhaustion policy.
    state: AtomicU64,
    /// Exact physical stream that admitted this request; never address-derived.
    session_owner: AtomicU64,
    response: UnsafeCell<MaybeUninit<CorrelationOutcome>>,
    waker: AtomicWaker,
    /// R-10: gates waker registration against allocation/recycle so a stale
    /// waiter cannot register (and overwrite the current owner's waker) once
    /// its id has been recycled. Held only for brief, await-free critical
    /// sections.
    register_gate: AtomicBool,
}

// Safety: access is synchronized via atomics and the correlation protocol.
unsafe impl Send for PendingResponseSlot {}
unsafe impl Sync for PendingResponseSlot {}

/// Shared state for correlation tracking
pub(crate) struct CorrelationTracker {
    /// Next wire ID. Zero is a permanent exhausted sentinel: a tracker may
    /// never reissue an ID, even after its slot has been released (REMOTE-3/4).
    next_id: AtomicU32,
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

#[inline]
fn correlation_slot_word(correlation_id: u32, state: u64) -> u64 {
    (u64::from(correlation_id) << 2) | state
}

impl CorrelationTracker {
    #[inline]
    fn slot_index(correlation_id: u32) -> usize {
        (correlation_id as usize) & PENDING_RESPONSES_MASK
    }

    #[inline]
    fn try_take_ready(slot_ref: &PendingResponseSlot, correlation_id: u32) -> ReadyTake {
        Self::try_take_ready_before_release(slot_ref, correlation_id, || {})
    }

    #[inline]
    fn try_take_ready_before_release(
        slot_ref: &PendingResponseSlot,
        correlation_id: u32,
        before_slot_release: impl FnOnce(),
    ) -> ReadyTake {
        match slot_ref.state.compare_exchange(
            correlation_slot_word(correlation_id, SLOT_READY),
            correlation_slot_word(correlation_id, SLOT_WRITING),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                // SAFETY: the full-ID READY -> WRITING CAS gives this reader
                // exclusive ownership until the payload has been moved out.
                let outcome = unsafe { (*slot_ref.response.get()).assume_init_read() };
                before_slot_release();
                slot_ref.state.store(SLOT_EMPTY, Ordering::Release);
                ReadyTake::Taken(outcome)
            }
            Err(word) if word & 3 == SLOT_READY && (word >> 2) as u32 != correlation_id => {
                ReadyTake::ForeignReady
            }
            Err(_) => ReadyTake::NotReady,
        }
    }

    /// R-10: atomically (verify ownership + register waker) under the per-slot
    /// nonblocking registration gate. Returns true if we still own the slot,
    /// false if the
    /// slot was recycled to a different id (the caller must stop waiting). The
    /// gate serializes this with `allocate()`'s id install, so a stale waiter
    /// whose id was recycled cannot overwrite the current owner's waker.
    #[inline]
    fn register_if_owner(
        slot_ref: &PendingResponseSlot,
        waker: &std::task::Waker,
        correlation_id: u32,
    ) -> bool {
        if slot_ref
            .register_gate
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            // A single bounded retry on the executor, never a blocking lock
            // or a spin waiting for a preempted registration/allocation.
            waker.wake_by_ref();
            return true;
        }
        let owned = (slot_ref.state.load(Ordering::Acquire) >> 2) as u32 == correlation_id;
        if owned {
            slot_ref.waker.register(waker);
        }
        slot_ref.register_gate.store(false, Ordering::Release);
        owned
    }

    fn new() -> Arc<Self> {
        debug_assert!(
            PENDING_RESPONSES_SIZE.is_power_of_two(),
            "PENDING_RESPONSES_SIZE must be power of two"
        );
        let mut pending = Vec::with_capacity(PENDING_RESPONSES_SIZE);
        pending.resize_with(PENDING_RESPONSES_SIZE, || PendingResponseSlot {
            state: AtomicU64::new(SLOT_EMPTY),
            session_owner: AtomicU64::new(0),
            response: UnsafeCell::new(MaybeUninit::uninit()),
            waker: AtomicWaker::new(),
            register_gate: AtomicBool::new(false),
        });
        Arc::new(Self {
            next_id: AtomicU32::new(1),
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
    /// Returns [`Err(NoFreeSlots)`](NoFreeSlots) when the admission probe window is in
    /// a non-EMPTY state. Previously this method was an unbounded `loop {}`
    /// which monopolised single-threaded tokio runtimes when slots leaked
    /// (production incident 2026-05-09).
    ///
    /// The returned guard borrows from `self` rather than holding an Arc
    /// clone, so the success path adds zero atomic refcount traffic over
    /// the previous bare-`u16` API.
    #[cfg(test)]
    pub(crate) fn allocate(&self) -> std::result::Result<SlotGuard<'_>, NoFreeSlots> {
        self.allocate_for_session(0)
    }

    /// Bind cleanup authority to the admitting stream (REMOTE-3/4). Zero is
    /// reserved for local/test callers without a physical transport.
    pub(crate) fn allocate_for_session(
        &self,
        instance_id: u64,
    ) -> std::result::Result<SlotGuard<'_>, NoFreeSlots> {
        // Bounded attempts across ID reservation and slot selection: both
        // contention and a full ring return typed backpressure, never spin
        // indefinitely. Performance acceptance requires measured evidence.
        for _ in 0..CORRELATION_ADMISSION_PROBES {
            // Atomically reserve the full wire identity, including the final
            // u32::MAX ID. The transition to zero is permanent; it is not a
            // wrap-to-one loop. Existing callers translate NoFreeSlots into
            // typed backpressure without evicting a healthy transport.
            let id = self.next_id.load(Ordering::Relaxed);
            if id == 0 {
                return Err(NoFreeSlots);
            }
            if self
                .next_id
                .compare_exchange(id, id.wrapping_add(1), Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
            {
                continue;
            }

            let slot = Self::slot_index(id);
            let slot_ref = &self.pending[slot];
            if slot_ref
                .register_gate
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                continue;
            }
            // Install the full ID together with exclusive WRITING ownership.
            // Completion cannot claim it until WAITING is published below.
            if slot_ref
                .state
                .compare_exchange(
                    SLOT_EMPTY,
                    correlation_slot_word(id, SLOT_WRITING),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                slot_ref.waker.wake();
                slot_ref.session_owner.store(instance_id, Ordering::Relaxed);
                let published = slot_ref
                    .state
                    .compare_exchange(
                        correlation_slot_word(id, SLOT_WRITING),
                        correlation_slot_word(id, SLOT_WAITING),
                        Ordering::Release,
                        Ordering::Relaxed,
                    )
                    .is_ok();
                if !published {
                    slot_ref.state.store(SLOT_EMPTY, Ordering::Release);
                }
                slot_ref.register_gate.store(false, Ordering::Release);
                if !published {
                    continue;
                }
                #[cfg(feature = "trace-correlation")]
                trace!(
                    "CorrelationTracker: Allocated correlation_id {} in slot {}",
                    id, slot
                );
                return Ok(SlotGuard { tracker: self, id });
            }

            slot_ref.register_gate.store(false, Ordering::Release);

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

    /// Claim only the exact request's WAITING state. A stale contender must
    /// fail without transiently blocking the genuine owner's completion.
    fn claim_slot_for_completion(&self, correlation_id: u32) -> Option<&PendingResponseSlot> {
        self.claim_slot_for_completion_before_check(correlation_id, || {})
    }

    #[inline]
    fn claim_slot_for_completion_before_check(
        &self,
        correlation_id: u32,
        after_claim: impl FnOnce(),
    ) -> Option<&PendingResponseSlot> {
        let slot_ref = &self.pending[Self::slot_index(correlation_id)];
        let claimed = slot_ref.state.compare_exchange(
            correlation_slot_word(correlation_id, SLOT_WAITING),
            correlation_slot_word(correlation_id, SLOT_WRITING),
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        after_claim();
        claimed.ok().map(|_| slot_ref)
    }

    /// Timeout release uses the same atomic full-identity boundary as reply
    /// publication, without acquiring a successor's temporary WRITING state.
    #[inline]
    fn try_expire_waiting(slot_ref: &PendingResponseSlot, correlation_id: u32) -> bool {
        slot_ref
            .state
            .compare_exchange(
                correlation_slot_word(correlation_id, SLOT_WAITING),
                SLOT_EMPTY,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Publish a claimed slot's outcome as READY and wake its waiter.
    fn publish_outcome(
        slot_ref: &PendingResponseSlot,
        correlation_id: u32,
        outcome: CorrelationOutcome,
    ) {
        unsafe {
            (*slot_ref.response.get()).write(outcome);
        }
        if slot_ref
            .state
            .compare_exchange(
                correlation_slot_word(correlation_id, SLOT_WRITING),
                correlation_slot_word(correlation_id, SLOT_READY),
                Ordering::Release,
                Ordering::Acquire,
            )
            .is_err()
        {
            // Cancellation delegates destruction to the exclusive producer;
            // it never waits for or races access to the payload cell.
            unsafe {
                (*slot_ref.response.get()).assume_init_drop();
            }
            slot_ref.state.store(SLOT_EMPTY, Ordering::Release);
        }
        slot_ref.waker.wake();
    }

    /// Complete a pending request with a response.
    ///
    /// Returns true when the response was consumed and published.
    pub(crate) fn complete(
        &self,
        correlation_id: u32,
        response: &mut Option<crate::AlignedBytes>,
    ) -> bool {
        self.complete_attributed(correlation_id, response, None)
    }

    /// [`Self::complete`], additionally snapshotting `completed_by` -- the
    /// authenticated identity of the connection whose read context is
    /// completing this reply -- into the slot together with the payload.
    /// `None` publishes a plain, unattributed response.
    pub(crate) fn complete_attributed(
        &self,
        correlation_id: u32,
        response: &mut Option<crate::AlignedBytes>,
        completed_by: Option<&Arc<crate::PeerId>>,
    ) -> bool {
        let Some(slot_ref) = self.claim_slot_for_completion(correlation_id) else {
            return false;
        };
        let Some(response) = response.take() else {
            slot_ref.state.store(SLOT_EMPTY, Ordering::Release);
            slot_ref.waker.wake();
            return false;
        };
        let outcome = match completed_by {
            Some(peer) => CorrelationOutcome::Attributed(response, Arc::clone(peer)),
            None => CorrelationOutcome::Response(response),
        };
        Self::publish_outcome(slot_ref, correlation_id, outcome);
        true
    }

    /// Complete a pending request with a NACK: the peer received the ask and
    /// explicitly declined or failed to answer it. Mirrors `complete`, so a
    /// waiter parked in `wait_for_response`/`wait_for_response_no_timeout`
    /// resolves immediately with `Err(GossipError::AskNacked(reason))`
    /// instead of hanging until its timeout fires.
    ///
    /// Returns true when the NACK was consumed and published.
    pub(crate) fn complete_nack(
        &self,
        correlation_id: u32,
        reason: crate::framing::AskNackReason,
    ) -> bool {
        let Some(slot_ref) = self.claim_slot_for_completion(correlation_id) else {
            return false;
        };
        Self::publish_outcome(slot_ref, correlation_id, CorrelationOutcome::Nack(reason));
        true
    }

    /// Cancel a pending request (used when send fails).
    ///
    /// Mirrors [`complete`](Self::complete): the slot's stored full
    /// correlation id is verified before the slot is released. `id` and
    /// `id + 8192*k` alias to the same slot index, so a stale cancel for a
    /// recycled id must not evict (or drop the ready response of) a
    /// *different* in-flight request occupying the same slot. A mismatched
    /// identity never changes state and no waker fires.
    pub(crate) fn cancel(&self, correlation_id: u32) {
        let slot = Self::slot_index(correlation_id);
        let slot_ref = &self.pending[slot];
        loop {
            let word = slot_ref.state.load(Ordering::Acquire);
            if (word >> 2) as u32 != correlation_id {
                return;
            }
            match word & 3 {
                SLOT_WAITING | SLOT_READY => {
                    if slot_ref
                        .state
                        .compare_exchange(
                            word,
                            correlation_slot_word(correlation_id, SLOT_WRITING),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        if word & 3 == SLOT_READY {
                            unsafe {
                                (*slot_ref.response.get()).assume_init_drop();
                            }
                        }
                        slot_ref.state.store(SLOT_EMPTY, Ordering::Release);
                        slot_ref.waker.wake();
                        return;
                    }
                }
                SLOT_WRITING => {
                    if word & SLOT_CANCEL_REQUESTED != 0
                        || slot_ref
                            .state
                            .compare_exchange(
                                word,
                                word | SLOT_CANCEL_REQUESTED,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                    {
                        return;
                    }
                }
                _ => return,
            }
        }
    }

    /// Retire only requests admitted by this exact physical session. The full
    /// request ID is captured before reading its owner and revalidated by
    /// cancel, so recycling the slot at any point cannot cancel a successor.
    pub(crate) fn cancel_session(&self, instance_id: u64) {
        for slot in &self.pending {
            let word = slot.state.load(Ordering::Acquire);
            // WRITING may be a not-yet-published allocation; its owner
            // field is not visible until WAITING. An already claimed reply
            // is allowed to finish and wake its own waiter.
            if matches!(word & 3, SLOT_WAITING | SLOT_READY)
                && slot.session_owner.load(Ordering::Relaxed) == instance_id
            {
                self.cancel((word >> 2) as u32);
            }
        }
    }

    /// Cancel all pending requests (used when a connection drops).
    #[cfg(test)]
    pub(crate) fn cancel_all(&self) {
        for slot in &self.pending {
            let word = slot.state.load(Ordering::Acquire);
            if word & 3 != SLOT_EMPTY {
                self.cancel((word >> 2) as u32);
            }
        }
    }

    async fn wait_for_response(
        &self,
        correlation_id: u32,
        timeout: Duration,
    ) -> Result<crate::AlignedBytes> {
        if timeout.is_zero() {
            self.cancel(correlation_id);
            return Err(crate::GossipError::Timeout);
        }
        let slot = Self::slot_index(correlation_id);
        let slot_ref = &self.pending[slot];

        let wait_fut = futures::future::poll_fn(|cx| {
            // If the slot was cancelled (e.g. connection dropped and cancel_all() ran),
            // return a concrete error instead of waiting forever.
            match Self::try_take_ready(slot_ref, correlation_id) {
                ReadyTake::Taken(outcome) => {
                    return std::task::Poll::Ready(outcome.into_result());
                }
                ReadyTake::ForeignReady => {
                    return std::task::Poll::Ready(Err(crate::GossipError::ConnectionDropped));
                }
                ReadyTake::NotReady => {}
            }
            let state = slot_ref.state.load(Ordering::Acquire);
            if state == SLOT_EMPTY {
                return std::task::Poll::Ready(Err(crate::GossipError::ConnectionDropped));
            }

            // R-10: register only while we still own the slot. register_if_owner
            // atomically (id-check + register) under the per-slot gate, so a
            // stale waiter whose id was recycled cannot overwrite the current
            // owner's waker. If recycled, terminate instead of parking.
            if !Self::register_if_owner(slot_ref, cx.waker(), correlation_id) {
                return std::task::Poll::Ready(Err(crate::GossipError::ConnectionDropped));
            }

            match Self::try_take_ready(slot_ref, correlation_id) {
                ReadyTake::Taken(outcome) => {
                    return std::task::Poll::Ready(outcome.into_result());
                }
                ReadyTake::ForeignReady => {
                    return std::task::Poll::Ready(Err(crate::GossipError::ConnectionDropped));
                }
                ReadyTake::NotReady => {}
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
                // REMOTE-4: timeout may release only this exact identity;
                // reject a stale timeout without claiming its successor.
                if Self::try_expire_waiting(slot_ref, correlation_id) {
                    return Err(crate::GossipError::Timeout);
                }
                match Self::try_take_ready(slot_ref, correlation_id) {
                    ReadyTake::Taken(outcome) => return outcome.into_result(),
                    ReadyTake::ForeignReady => return Err(crate::GossipError::ConnectionDropped),
                    ReadyTake::NotReady => {}
                }
                match slot_ref.state.load(Ordering::Acquire) & 3 {
                    SLOT_WRITING => {
                        self.cancel(correlation_id);
                        Err(crate::GossipError::Timeout)
                    }
                    SLOT_EMPTY => Err(crate::GossipError::ConnectionDropped),
                    _ => Err(crate::GossipError::Timeout),
                }
            }
        }
    }

    async fn wait_for_response_no_timeout(
        &self,
        correlation_id: u32,
    ) -> Result<crate::AlignedBytes> {
        self.wait_for_response_no_timeout_outcome(correlation_id)
            .await
            .and_then(CorrelationOutcome::into_result)
    }

    /// Wait for the slot's raw outcome (payload, attributed payload, or NACK)
    /// without converting it, so callers can choose how to interpret it.
    async fn wait_for_response_no_timeout_outcome(
        &self,
        correlation_id: u32,
    ) -> Result<CorrelationOutcome> {
        let slot = Self::slot_index(correlation_id);
        let slot_ref = &self.pending[slot];

        futures::future::poll_fn(|cx| {
            match Self::try_take_ready(slot_ref, correlation_id) {
                ReadyTake::Taken(outcome) => {
                    return std::task::Poll::Ready(Ok(outcome));
                }
                ReadyTake::ForeignReady => {
                    return std::task::Poll::Ready(Err(crate::GossipError::ConnectionDropped));
                }
                ReadyTake::NotReady => {}
            }
            let state = slot_ref.state.load(Ordering::Acquire);
            if state == SLOT_EMPTY {
                return std::task::Poll::Ready(Err(crate::GossipError::ConnectionDropped));
            }
            // R-10: register only while we still own the slot. register_if_owner
            // atomically (id-check + register) under the per-slot gate, so a
            // stale waiter whose id was recycled cannot overwrite the current
            // owner's waker. If recycled, terminate instead of parking.
            if !Self::register_if_owner(slot_ref, cx.waker(), correlation_id) {
                return std::task::Poll::Ready(Err(crate::GossipError::ConnectionDropped));
            }
            match Self::try_take_ready(slot_ref, correlation_id) {
                ReadyTake::Taken(outcome) => {
                    return std::task::Poll::Ready(Ok(outcome));
                }
                ReadyTake::ForeignReady => {
                    return std::task::Poll::Ready(Err(crate::GossipError::ConnectionDropped));
                }
                ReadyTake::NotReady => {}
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
            "correlation tracker exhausted: no response slot or fresh wire ID available"
        )
    }
}

impl std::error::Error for NoFreeSlots {}

/// RAII guard for a correlation slot reservation.
///
/// Holds a shared borrow of the tracker (no Arc clone) and a single 32-bit
/// id. On drop the slot is cancelled — this is what closes the production
/// leak where a future awaiting [`CorrelationTracker::wait_for_response`]
/// could be cancelled mid-await (outer `tokio::time::timeout`, `select!`
/// arm losing) without restoring slot state.
///
/// Call [`SlotGuard::disarm`] on the success path to consume the guard
/// without running the cancellation Drop. The disarm path uses
/// `mem::forget`, so the success path adds zero atomic ops over the
/// previous bare-id API.
#[must_use = "dropping a SlotGuard cancels the slot; call .disarm() on success"]
pub(crate) struct SlotGuard<'a> {
    tracker: &'a CorrelationTracker,
    id: u32,
}

impl<'a> SlotGuard<'a> {
    #[inline(always)]
    pub(crate) fn id(&self) -> u32 {
        self.id
    }

    /// Consume the guard without running the cancellation Drop. Use this
    /// after the consumer has moved the slot out of `SLOT_WAITING`
    /// (i.e. on the success path of `wait_for_response`).
    #[inline(always)]
    pub(crate) fn disarm(self) -> u32 {
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

#[cfg(test)]
mod correlation_tests {
    use std::sync::Arc;

    use super::*;

    /// Unwrap a `ReadyTake::Taken` holding a real response, panicking
    /// otherwise (test helper).
    fn expect_taken(take: ReadyTake, msg: &str) -> crate::AlignedBytes {
        match take {
            ReadyTake::Taken(CorrelationOutcome::Response(response))
            | ReadyTake::Taken(CorrelationOutcome::Attributed(response, _)) => response,
            ReadyTake::Taken(CorrelationOutcome::Nack(reason)) => {
                panic!("{msg}: slot held a NACK ({reason}), not a response")
            }
            ReadyTake::NotReady => panic!("{msg}: slot was not READY (NotReady)"),
            ReadyTake::ForeignReady => panic!("{msg}: slot held a foreign id (ForeignReady)"),
        }
    }

    #[test]
    fn warm_correlation_register_complete_and_cancel_have_no_metadata_allocations() {
        use crate::ask_forwarder::allocation_evidence;

        let tracker = CorrelationTracker::new();
        let waker = futures::task::noop_waker();
        // Explicit diagnostic subscribers are outside the transport cost model.
        // Keep this thread deterministic even when another test enables tracing.
        let dispatch = tracing::Dispatch::none();
        let _dispatch = tracing::dispatcher::set_default(&dispatch);
        let warm = tracker.allocate_for_session(71).unwrap();
        drop(warm);
        let (completed, counts) = allocation_evidence::measure(|| {
            let mut completed = 0;
            for index in 0..1024 {
                let guard = tracker.allocate_for_session(71).unwrap();
                let id = guard.id();
                let slot = &tracker.pending[CorrelationTracker::slot_index(id)];
                assert!(CorrelationTracker::register_if_owner(slot, &waker, id));
                if index % 2 == 0 {
                    assert!(tracker.complete_nack(id, crate::framing::AskNackReason::UnknownActor));
                    assert!(matches!(
                        CorrelationTracker::try_take_ready(slot, id),
                        ReadyTake::Taken(CorrelationOutcome::Nack(
                            crate::framing::AskNackReason::UnknownActor
                        ))
                    ));
                    completed += 1;
                } else {
                    tracker.cancel(id);
                    assert_eq!(slot.state.load(Ordering::Acquire), SLOT_EMPTY);
                }
            }
            completed
        });
        assert_eq!(completed, 512, "both completion and cancellation controls must run");
        assert_eq!(counts.allocations, 0);
        assert_eq!(counts.reallocations, 0);
        assert_eq!(tracker.next_id.load(Ordering::Acquire), 1026);
        assert!(tracker.allocate_for_session(72).is_ok());
    }

    /// REMOTE-3/4: exhausting the wire namespace must never recycle an ID.
    #[test]
    fn saturated_correlation_admission_has_a_fixed_probe_budget() {
        let tracker = CorrelationTracker::new();
        let guards: Vec<_> = (0..PENDING_RESPONSES_SIZE)
            .map(|_| tracker.allocate().unwrap())
            .collect();
        let before = tracker.next_id.load(Ordering::Relaxed);
        assert!(tracker.allocate().is_err());
        assert_eq!(
            tracker.next_id.load(Ordering::Relaxed) - before,
            CORRELATION_ADMISSION_PROBES as u32,
        );
        drop(guards);
        assert!(tracker.allocate().is_ok());
    }

    #[test]
    fn cancellation_of_claimed_publication_never_spins_or_releases_early() {
        let tracker = CorrelationTracker::new();
        let guard = tracker.allocate().unwrap();
        let id = guard.id();
        let claimed = tracker.claim_slot_for_completion(id).unwrap();
        tracker.cancel(id);
        assert_ne!(claimed.state.load(Ordering::Acquire), SLOT_EMPTY);
        CorrelationTracker::publish_outcome(
            claimed,
            id,
            CorrelationOutcome::Nack(crate::framing::AskNackReason::UnknownActor),
        );
        assert_eq!(claimed.state.load(Ordering::Acquire), SLOT_EMPTY);
    }

    #[test]
    fn stale_exit_sweep_preserves_successor_requests() {
        let tracker = CorrelationTracker::new();
        let old = tracker.allocate_for_session(11).unwrap();
        let successor = tracker.allocate_for_session(12).unwrap();
        let foreign = tracker.allocate_for_session(13).unwrap();
        tracker.cancel_session(11);
        assert_eq!(
            tracker.pending[CorrelationTracker::slot_index(old.id())]
                .state
                .load(Ordering::Acquire),
            SLOT_EMPTY
        );
        assert!(tracker.complete_nack(successor.id(), crate::framing::AskNackReason::UnknownActor));
        assert!(tracker.complete_nack(foreign.id(), crate::framing::AskNackReason::UnknownActor));
        tracker.cancel_session(11);
        assert!(matches!(
            CorrelationTracker::try_take_ready(
                &tracker.pending[CorrelationTracker::slot_index(successor.id())],
                successor.id()
            ),
            ReadyTake::Taken(CorrelationOutcome::Nack(_))
        ));
    }

    #[test]
    fn qa_corr_002_wire_id_exhaustion_is_permanent() {
        let tracker = CorrelationTracker::new();
        let early = tracker.allocate().unwrap();
        let early_id = early.id();
        drop(early);
        tracker.next_id.store(u32::MAX, Ordering::Relaxed);
        let last = tracker.allocate().unwrap();
        assert_eq!(last.id(), u32::MAX);
        drop(last);
        for _ in 0..3 {
            assert!(tracker.allocate().is_err(), "exhaustion must not wrap to ID 1");
        }
        assert_eq!(tracker.next_id.load(Ordering::Relaxed), 0);
        assert!(!tracker.complete_nack(early_id, crate::framing::AskNackReason::NoDispatcher));
        assert!(CorrelationTracker::new().allocate().is_ok(), "a distinct tracker has a fresh namespace");
    }

    #[test]
    fn qa_corr_002_concurrent_last_id_is_issued_once() {
        let tracker = CorrelationTracker::new();
        tracker.next_id.store(u32::MAX, Ordering::Relaxed);
        let start = std::sync::Barrier::new(8);
        let issued = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    start.wait();
                    if let Ok(slot) = tracker.allocate() {
                        assert_eq!(slot.id(), u32::MAX);
                        issued.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });
        assert_eq!(issued.load(Ordering::Relaxed), 1);
        assert!(tracker.allocate().is_err());
    }

    /// REMOTE-3/4: exercise the precise claim-to-ID-check window without
    /// threads or sleeps. Under the old state-only CAS, the stale claimant
    /// holds Y in WRITING during the callback and Y's sole response is lost.
    #[test]
    fn qa_corr_003_stale_claim_cannot_suppress_successor_completion() {
        let tracker = CorrelationTracker::new();
        let old = tracker.allocate().unwrap();
        let old_id = old.id();
        drop(old);
        tracker
            .next_id
            .store(old_id + PENDING_RESPONSES_SIZE as u32, Ordering::Relaxed);
        let current = tracker.allocate().unwrap();
        let current_id = current.id();
        assert_ne!(old_id, current_id);
        assert_eq!(
            CorrelationTracker::slot_index(old_id),
            CorrelationTracker::slot_index(current_id)
        );
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let mut response = Some(crate::AlignedBytes::from_pooled_slice(b"current", pool).unwrap());
        let mut completed = false;
        let stale = tracker.claim_slot_for_completion_before_check(old_id, || {
            completed = tracker.complete(current_id, &mut response);
        });
        assert!(stale.is_none());
        assert!(
            completed,
            "stale claim must not temporarily exclude the genuine reply"
        );
        assert!(response.is_none());
        let slot_ref = &tracker.pending[CorrelationTracker::slot_index(current_id)];
        assert_eq!(
            expect_taken(
                CorrelationTracker::try_take_ready(slot_ref, current_id),
                "successor response"
            )
            .as_ref(),
            b"current"
        );
        current.disarm();
    }

    /// The public internal operations must preserve the same packed owner,
    /// including zero-duration timeouts (which take the cancellation path).
    #[tokio::test]
    async fn qa_corr_003_stale_cancel_completion_and_timeout_preserve_owner() {
        let tracker = CorrelationTracker::new();
        let current = tracker.allocate().unwrap();
        let id = current.id();
        let stale_id = id + PENDING_RESPONSES_SIZE as u32;
        let slot_ref = &tracker.pending[CorrelationTracker::slot_index(id)];
        let waiting = correlation_slot_word(id, SLOT_WAITING);
        tracker.cancel(stale_id);
        assert_eq!(slot_ref.state.load(Ordering::Acquire), waiting);
        assert!(!CorrelationTracker::try_expire_waiting(slot_ref, stale_id));
        assert_eq!(slot_ref.state.load(Ordering::Acquire), waiting);
        assert!(matches!(
            tracker.wait_for_response(stale_id, Duration::ZERO).await,
            Err(crate::GossipError::Timeout)
        ));
        assert_eq!(slot_ref.state.load(Ordering::Acquire), waiting);
        assert!(!tracker.complete_nack(stale_id, crate::framing::AskNackReason::UnknownActor));
        assert_eq!(slot_ref.state.load(Ordering::Acquire), waiting);
        assert!(tracker.complete_nack(id, crate::framing::AskNackReason::UnknownActor));
        tracker.cancel(stale_id);
        assert_eq!(
            slot_ref.state.load(Ordering::Acquire),
            correlation_slot_word(id, SLOT_READY)
        );
        assert!(matches!(
            tracker.wait_for_response_no_timeout(id).await,
            Err(crate::GossipError::AskNacked(
                crate::framing::AskNackReason::UnknownActor
            ))
        ));
        current.disarm();
        let expires = tracker.allocate().unwrap();
        let expires_id = expires.id();
        assert!(CorrelationTracker::try_expire_waiting(
            &tracker.pending[CorrelationTracker::slot_index(expires_id)],
            expires_id
        ));
        let own = tracker.allocate().unwrap();
        let own_id = own.id();
        assert!(matches!(
            tracker.wait_for_response(own_id, Duration::ZERO).await,
            Err(crate::GossipError::Timeout)
        ));
        assert_eq!(
            tracker.pending[CorrelationTracker::slot_index(own_id)]
                .state
                .load(Ordering::Acquire),
            SLOT_EMPTY
        );
    }

    /// A NACK must reach the waiter as an immediate typed error, not a
    /// timeout: `complete_nack` publishes the slot exactly like `complete`
    /// does for a real payload, but `wait_for_response` translates it into
    /// `Err(GossipError::AskNacked(reason))` instead of `Ok(bytes)`.
    #[tokio::test]
    async fn complete_nack_delivers_a_typed_error_instead_of_a_timeout() {
        let tracker = CorrelationTracker::new();
        let guard = tracker.allocate().expect("slot should allocate");
        let id = guard.id();

        assert!(tracker.complete_nack(id, crate::framing::AskNackReason::UnknownActor));

        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            tracker.wait_for_response_no_timeout(id),
        )
        .await
        .expect("a NACK must resolve immediately, not hang until the timeout fires");

        assert!(
            matches!(
                outcome,
                Err(crate::GossipError::AskNacked(
                    crate::framing::AskNackReason::UnknownActor
                ))
            ),
            "expected an immediate typed NACK error, got {outcome:?}"
        );
    }

    #[test]
    fn dropping_guard_after_a_raced_response_releases_the_slot() {
        let tracker = CorrelationTracker::new();
        let guard = tracker.allocate().expect("slot should allocate");
        let id = guard.id();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let mut response = Some(crate::AlignedBytes::from_pooled_slice(b"reply", pool).unwrap());
        assert!(tracker.complete(id, &mut response));
        assert!(response.is_none());

        drop(guard);
        let slot = CorrelationTracker::slot_index(id);
        assert_eq!(
            tracker.pending[slot].state.load(Ordering::Acquire) & 3,
            SLOT_EMPTY,
            "error-path guard drop must clear a raced ready response"
        );
    }

    #[test]
    fn correlation_ids_do_not_repeat_at_the_u16_boundary() {
        let tracker = CorrelationTracker::new();
        tracker
            .next_id
            .store(u32::from(u16::MAX), Ordering::Relaxed);

        let last_u16 = tracker.allocate().expect("u16::MAX should allocate");
        assert_eq!(last_u16.id(), u32::from(u16::MAX));
        drop(last_u16);

        let after_boundary = tracker.allocate().expect("next id should allocate");
        assert!(
            after_boundary.id() > u32::from(u16::MAX),
            "a correlation id must never repeat after the old 16-bit space is exhausted"
        );
    }

    #[test]
    fn stale_cancel_for_an_aliased_id_must_not_evict_a_waiting_request() {
        let tracker = CorrelationTracker::new();
        let guard = tracker.allocate().expect("slot should allocate");
        let id = guard.id();

        // `id` and `id + 8192` alias to the same slot index. A stale cancel
        // for the recycled aliased id must be a no-op for the *different*
        // request currently occupying the slot.
        tracker.cancel(id + PENDING_RESPONSES_SIZE as u32);

        let pool = Arc::new(crate::AlignedBytesPool::default());
        let mut response = Some(crate::AlignedBytes::from_pooled_slice(b"reply", pool).unwrap());
        assert!(
            tracker.complete(id, &mut response),
            "a stale cancel for an aliased correlation id evicted a waiting request"
        );

        // Drain the ready response so drop(guard) sees a clean slot.
        let slot_ref = &tracker.pending[CorrelationTracker::slot_index(id)];
        let taken = CorrelationTracker::try_take_ready(slot_ref, id);
        assert_eq!(expect_taken(taken, "ready response").as_ref(), b"reply");
        guard.disarm();
    }

    #[test]
    fn stale_cancel_for_an_aliased_id_must_not_drop_a_ready_response() {
        let tracker = CorrelationTracker::new();
        let guard = tracker.allocate().expect("slot should allocate");
        let id = guard.id();
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let mut response = Some(crate::AlignedBytes::from_pooled_slice(b"reply", pool).unwrap());
        assert!(tracker.complete(id, &mut response));

        // The slot is READY with `id`'s response stored; a stale cancel for
        // the aliased id must not drop it.
        tracker.cancel(id + PENDING_RESPONSES_SIZE as u32);

        let slot_ref = &tracker.pending[CorrelationTracker::slot_index(id)];
        let taken = CorrelationTracker::try_take_ready(slot_ref, id);
        assert_eq!(
            expect_taken(
                taken,
                "a stale cancel for an aliased correlation id dropped a ready response"
            )
            .as_ref(),
            b"reply"
        );
        guard.disarm();
    }

    #[test]
    fn ready_slot_remains_exclusively_owned_until_response_is_read() {
        let tracker = CorrelationTracker::new();
        let guard = tracker.allocate().expect("slot should allocate");
        let id = guard.id();
        let slot = CorrelationTracker::slot_index(id);
        let slot_ref = &tracker.pending[slot];
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let mut response = Some(crate::AlignedBytes::from_pooled_slice(b"reply", pool).unwrap());
        assert!(tracker.complete(id, &mut response));

        let taken = CorrelationTracker::try_take_ready_before_release(slot_ref, id, || {
            assert_eq!(
                slot_ref.state.load(Ordering::Acquire) & 3,
                SLOT_WRITING,
                "the slot must not be reusable while its response is being read"
            );
        });

        assert_eq!(expect_taken(taken, "ready response").as_ref(), b"reply");
        assert_eq!(slot_ref.state.load(Ordering::Acquire) & 3, SLOT_EMPTY);
        guard.disarm();
    }

    /// R-10: a stale waiter whose aliased id maps to a slot READY with a
    /// *different* request's response must NOT steal it (the #130 fix covered
    /// `complete`/`cancel`; the waiter-side `try_take_ready` did not verify the
    /// id). `id` and `id + 8192` share a slot index.
    #[test]
    fn qa_r10_stale_waiter_cannot_steal_a_recycled_slot_response() {
        let tracker = CorrelationTracker::new();
        let guard = tracker.allocate().expect("slot should allocate");
        let id = guard.id();
        let slot = CorrelationTracker::slot_index(id);
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let mut response = Some(crate::AlignedBytes::from_pooled_slice(b"reply", pool).unwrap());
        assert!(tracker.complete(id, &mut response));

        let stale_id = id + PENDING_RESPONSES_SIZE as u32;
        assert_eq!(CorrelationTracker::slot_index(stale_id), slot);
        let slot_ref = &tracker.pending[slot];
        assert!(
            matches!(
                CorrelationTracker::try_take_ready(slot_ref, stale_id),
                ReadyTake::ForeignReady
            ),
            "stale waiter must not steal a recycled slot's response (R-10)"
        );
        // The genuine owner still receives it.
        assert_eq!(
            expect_taken(
                CorrelationTracker::try_take_ready(slot_ref, id),
                "genuine owner's response"
            )
            .as_ref(),
            b"reply"
        );
        guard.disarm();
    }

    /// R-10: a stale waiter on an aliased slot must terminate WITHOUT evicting
    /// the different request currently WAITING there. With the id check before
    /// waker registration, the stale waiter returns ConnectionDropped as soon as
    /// it observes the slot no longer holds its id (previously the timeout path
    /// did a bare WAITING -> EMPTY CAS with no id check).
    #[tokio::test]
    async fn qa_r10_stale_waiter_does_not_evict_an_aliased_waiter() {
        let tracker = CorrelationTracker::new();
        let guard = tracker.allocate().expect("slot should allocate");
        let id = guard.id();
        let slot = CorrelationTracker::slot_index(id);
        // Slot is WAITING for `id`; `id + 8192` aliases to the same slot.
        let stale_id = id + PENDING_RESPONSES_SIZE as u32;
        let outcome = tracker
            .wait_for_response(stale_id, std::time::Duration::from_millis(1))
            .await;
        assert!(
            matches!(outcome, Err(crate::GossipError::ConnectionDropped)),
            "stale waiter must terminate with ConnectionDropped, got {outcome:?}"
        );
        let slot_ref = &tracker.pending[slot];
        assert_eq!(
            slot_ref.state.load(Ordering::Acquire) & 3,
            SLOT_WAITING,
            "aliased WAITING entry must survive a stale waiter (R-10)"
        );
        assert_eq!(
            (slot_ref.state.load(Ordering::Acquire) >> 2) as u32,
            id,
            "the slot must still belong to id, not the stale waiter"
        );
        guard.disarm();
    }

    /// A `Waker` that counts wake() calls, so a test can observe *which*
    /// registered waker `complete()` actually woke.
    struct CountWaker(Arc<std::sync::atomic::AtomicUsize>);
    impl std::task::Wake for CountWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    fn count_waker() -> (std::task::Waker, Arc<std::sync::atomic::AtomicUsize>) {
        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let waker: std::task::Waker = Arc::new(CountWaker(counter.clone())).into();
        (waker, counter)
    }

    /// R-10 (waker-ownership race, fail-first): after a slot is recycled from
    /// X to Y and Y registers its waker, a stale X attempting to register MUST
    /// NOT overwrite Y's waker. `complete(Y)` must wake Y (not X), so Y
    /// receives its reply. Pre-fix (bare `register` with no id gate) X overwrote
    /// Y's waker and `complete(Y)` woke X while Y hung -- proven RED by reverting
    /// `register_if_owner` to a bare register (X's counter fires, Y's does not).
    #[test]
    fn qa_r10_stale_waiter_cannot_overwrite_the_owners_waker() {
        let tracker = CorrelationTracker::new();
        // 1. Allocate X in slot S, then recycle S to Y (cancel X, force the
        //    next allocation to alias to S, allocate Y).
        let guard_x = tracker.allocate().expect("slot should allocate");
        let id_x = guard_x.id();
        let slot = CorrelationTracker::slot_index(id_x);
        drop(guard_x); // cancel(X): S -> EMPTY
        tracker
            .next_id
            .store(id_x + PENDING_RESPONSES_SIZE as u32, Ordering::Release);
        let guard_y = tracker.allocate().expect("Y should re-claim slot S");
        let id_y = guard_y.id();
        assert_eq!(
            CorrelationTracker::slot_index(id_y),
            slot,
            "Y must alias to S"
        );
        assert_ne!(id_x, id_y);

        let slot_ref = &tracker.pending[slot];
        // 2. Y registers its waker.
        let (waker_y, y_wakes) = count_waker();
        assert!(
            CorrelationTracker::register_if_owner(slot_ref, &waker_y, id_y),
            "Y (the current owner) must register"
        );
        // 3. Stale X attempts to register on the same slot.
        let (waker_x, x_wakes) = count_waker();
        assert!(
            !CorrelationTracker::register_if_owner(slot_ref, &waker_x, id_x),
            "stale X must be refused registration after recycle (R-10)"
        );
        // 4. complete(Y) must wake Y, never X.
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let mut response = Some(crate::AlignedBytes::from_pooled_slice(b"for-Y", pool).unwrap());
        assert!(tracker.complete(id_y, &mut response));
        assert_eq!(
            y_wakes.load(Ordering::SeqCst),
            1,
            "complete(Y) must wake Y's waker"
        );
        assert_eq!(
            x_wakes.load(Ordering::SeqCst),
            0,
            "X's waker must not have been registered (no overwrite)"
        );
        // 5. Y receives its reply.
        assert_eq!(
            expect_taken(
                CorrelationTracker::try_take_ready(slot_ref, id_y),
                "Y's reply"
            )
            .as_ref(),
            b"for-Y"
        );
        guard_y.disarm();
    }

    /// R-10 (liveness, no-timeout path): the genuine owner on the no-timeout
    /// path (`handle.rs::ask_direct_no_timeout`) must still receive its reply
    /// promptly after a recycle -- it must not hang because a stale waiter
    /// overwrote its waker. Watchdog-bounded.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn qa_r10_owner_receives_reply_on_no_timeout_path_after_recycle() {
        let tracker = Arc::new(CorrelationTracker::new());
        let guard_x = tracker.allocate().expect("slot should allocate");
        let id_x = guard_x.id();
        let slot = CorrelationTracker::slot_index(id_x);
        drop(guard_x); // recycle: S -> EMPTY
        tracker
            .next_id
            .store(id_x + PENDING_RESPONSES_SIZE as u32, Ordering::Release);
        let guard_y = tracker.allocate().expect("Y should re-claim slot S");
        let id_y = guard_y.id();
        assert_eq!(CorrelationTracker::slot_index(id_y), slot);

        // Y waits with NO timeout.
        let tracker_for_wait = tracker.clone();
        let y_wait =
            tokio::spawn(async move { tracker_for_wait.wait_for_response_no_timeout(id_y).await });
        // Let Y register its waker.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // A stale X attempts to register on the same slot -- refused by the gate.
        let (stale_waker, _) = count_waker();
        assert!(!CorrelationTracker::register_if_owner(
            &tracker.pending[slot],
            &stale_waker,
            id_x
        ));

        // complete(Y) wakes Y (not the stale X).
        let pool = Arc::new(crate::AlignedBytesPool::default());
        let mut response = Some(crate::AlignedBytes::from_pooled_slice(b"for-Y", pool).unwrap());
        assert!(tracker.complete(id_y, &mut response));

        // Y must receive its reply promptly -- the no-timeout path must not hang.
        let reply = tokio::time::timeout(std::time::Duration::from_secs(2), y_wait)
            .await
            .expect("Y's no-timeout wait must not hang (R-10)")
            .expect("Y's task joined");
        assert_eq!(reply.unwrap().as_ref(), b"for-Y");
        guard_y.disarm();
    }

    #[test]
    fn cancel_all_must_keep_slot_exclusive_until_payload_drop_finishes() {
        use std::sync::Barrier;

        #[repr(align(16))]
        struct BlockOnDrop {
            bytes: [u8; 16],
            entered: Arc<Barrier>,
            release: Arc<Barrier>,
        }
        impl AsRef<[u8]> for BlockOnDrop {
            fn as_ref(&self) -> &[u8] {
                &self.bytes
            }
        }
        impl Drop for BlockOnDrop {
            fn drop(&mut self) {
                self.entered.wait();
                self.release.wait();
            }
        }

        let tracker = CorrelationTracker::new();
        let id = tracker.allocate().unwrap().disarm();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let bytes = bytes::Bytes::from_owner(BlockOnDrop {
            bytes: [0; 16],
            entered: entered.clone(),
            release: release.clone(),
        });
        let mut response = Some(crate::AlignedBytes::from_bytes(bytes).unwrap());
        assert!(tracker.complete(id, &mut response));

        let cancel_tracker = tracker.clone();
        let cancel = std::thread::spawn(move || cancel_tracker.cancel_all());
        entered.wait();
        tracker
            .next_id
            .store(id + PENDING_RESPONSES_SIZE as u32, Ordering::Relaxed);
        let new_guard = tracker.allocate().unwrap();
        let reused =
            CorrelationTracker::slot_index(new_guard.id()) == CorrelationTracker::slot_index(id);
        release.wait();
        cancel.join().unwrap();
        drop(new_guard);
        assert!(
            !reused,
            "cancel_all published EMPTY before dropping the old payload"
        );
    }
}
