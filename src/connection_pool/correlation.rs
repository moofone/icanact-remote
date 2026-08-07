/// Maximum number of pending responses (must be power of 2 for fast modulo)
const PENDING_RESPONSES_SIZE: usize = 8192;
const PENDING_RESPONSES_MASK: usize = PENDING_RESPONSES_SIZE - 1;
const SLOT_EMPTY: u8 = 0;
const SLOT_WAITING: u8 = 1;
const SLOT_WRITING: u8 = 2;
const SLOT_READY: u8 = 3;

/// What a completed slot resolves to: a real payload, or an explicit NACK
/// from the peer. `Nack` is `Copy` (a tag + a one-byte reason), so publishing
/// it costs nothing beyond what `Response` already pays to publish a payload.
pub(crate) enum CorrelationOutcome {
    Response(crate::AlignedBytes),
    Nack(crate::framing::AskNackReason),
}

impl CorrelationOutcome {
    /// Convert to the `Result` a waiter actually receives: a NACK becomes an
    /// immediate typed error rather than a payload the caller must inspect.
    fn into_result(self) -> Result<crate::AlignedBytes> {
        match self {
            Self::Response(bytes) => Ok(bytes),
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
    state: AtomicU8,
    /// Full 32-bit correlation id of the request currently occupying this
    /// slot (meaningful whenever `state != SLOT_EMPTY`). Because `id` and
    /// `id + 8192*k` alias to the same slot index, the 13-bit slot index is
    /// not by itself a sufficient match — `complete()` verifies this full id
    /// so a stale/delayed response for a recycled id cannot complete a
    /// *different* in-flight request occupying the same slot.
    id: AtomicU32,
    response: UnsafeCell<MaybeUninit<CorrelationOutcome>>,
    waker: AtomicWaker,
    /// R-10: gates waker registration against allocation/recycle so a stale
    /// waiter cannot register (and overwrite the current owner's waker) once
    /// its id has been recycled. Held only for brief, await-free critical
    /// sections.
    register_lock: std::sync::Mutex<()>,
}

// Safety: access is synchronized via atomics and the correlation protocol.
unsafe impl Send for PendingResponseSlot {}
unsafe impl Sync for PendingResponseSlot {}

/// Shared state for correlation tracking
pub(crate) struct CorrelationTracker {
    /// Next correlation ID to use
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

impl CorrelationTracker {
    #[inline]
    fn slot_index(correlation_id: u32) -> usize {
        (correlation_id as usize) & PENDING_RESPONSES_MASK
    }

    #[inline]
    fn try_take_ready(
        slot_ref: &PendingResponseSlot,
        correlation_id: u32,
    ) -> ReadyTake {
        Self::try_take_ready_before_release(slot_ref, correlation_id, || {})
    }

    #[inline]
    fn try_take_ready_before_release(
        slot_ref: &PendingResponseSlot,
        correlation_id: u32,
        before_slot_release: impl FnOnce(),
    ) -> ReadyTake {
        if slot_ref
            .state
            .compare_exchange(
                SLOT_READY,
                SLOT_WRITING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            // R-10: verify the full correlation id under exclusive WRITING
            // ownership. `id` and `id + 8192*k` alias to the same slot index, so
            // after cancel_all + ~8192 further allocations a stale, woken
            // waiter's slot can be READY with a *different* request's response;
            // without this check the waiter steals that response (and the
            // rightful owner later gets ConnectionDropped). Load `id` with
            // Acquire so the value observed is the one whose store
            // happened-before the READY publication (a Relaxed load could read a
            // stale id and treat the genuine owner as foreign).
            if slot_ref.id.load(Ordering::Acquire) != correlation_id {
                // Restore READY for the genuine owner, nudge the slot waker once
                // (the owner may be parked), and tell this stale waiter to stop
                // waiting. Returning ForeignReady (-> ConnectionDropped)
                // terminates the stale wait, so it does not re-poll/self-wake on
                // the shared AtomicWaker (which would busy-loop and starve the
                // genuine owner).
                slot_ref.state.store(SLOT_READY, Ordering::Release);
                slot_ref.waker.wake();
                return ReadyTake::ForeignReady;
            }
            // SAFETY: READY -> WRITING gives this reader exclusive ownership;
            // allocation requires EMPTY and cancellation spins on WRITING.
            let outcome = unsafe { (*slot_ref.response.get()).assume_init_read() };
            before_slot_release();
            slot_ref.state.store(SLOT_EMPTY, Ordering::Release);
            return ReadyTake::Taken(outcome);
        }
        ReadyTake::NotReady
    }

    /// R-10: atomically (verify ownership + register waker) under the per-slot
    /// gate. Returns true if registered (we still own the slot), false if the
    /// slot was recycled to a different id (the caller must stop waiting). The
    /// gate serializes this with `allocate()`'s id install, so a stale waiter
    /// whose id was recycled cannot overwrite the current owner's waker.
    #[inline]
    fn register_if_owner(
        slot_ref: &PendingResponseSlot,
        waker: &std::task::Waker,
        correlation_id: u32,
    ) -> bool {
        let _gate = slot_ref.register_lock.lock().expect("register_lock poisoned");
        if slot_ref.id.load(Ordering::Acquire) != correlation_id {
            return false;
        }
        slot_ref.waker.register(waker);
        true
    }

    fn new() -> Arc<Self> {
        debug_assert!(
            PENDING_RESPONSES_SIZE.is_power_of_two(),
            "PENDING_RESPONSES_SIZE must be power of two"
        );
        let mut pending = Vec::with_capacity(PENDING_RESPONSES_SIZE);
        pending.resize_with(PENDING_RESPONSES_SIZE, || PendingResponseSlot {
            state: AtomicU8::new(SLOT_EMPTY),
            id: AtomicU32::new(0),
            response: UnsafeCell::new(MaybeUninit::uninit()),
            waker: AtomicWaker::new(),
            register_lock: std::sync::Mutex::new(()),
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
    /// Returns [`Err(NoFreeSlots)`](NoFreeSlots) when the entire ring is in
    /// a non-EMPTY state. Previously this method was an unbounded `loop {}`
    /// which monopolised single-threaded tokio runtimes when slots leaked
    /// (production incident 2026-05-09).
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
            // Claim the slot through the transient WRITING state so we can
            // publish this request's full id *before* the slot becomes
            // observable as WAITING. A concurrent `complete()` acquires the
            // WAITING state and is therefore guaranteed to see the id we store
            // here, letting it reject a mismatched (aliased) response.
            if slot_ref
                .state
                .compare_exchange(
                    SLOT_EMPTY,
                    SLOT_WRITING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                {
                    // R-10: under the registration gate, wake any waker a stale
                    // waiter left on this slot and install the new id before
                    // publishing WAITING. A concurrent waiter's
                    // register_if_owner takes the same gate, so it cannot observe
                    // a half-installed id or overwrite a waker for the previous
                    // generation.
                    let _gate =
                        slot_ref.register_lock.lock().expect("register_lock poisoned");
                    slot_ref.waker.wake();
                    slot_ref.id.store(id, Ordering::Release);
                }
                slot_ref.state.store(SLOT_WAITING, Ordering::Release);
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

    /// Claim exclusive (WRITING) ownership of `correlation_id`'s slot for
    /// publishing a completion, verifying the full id so a stale or delayed
    /// completion for a recycled id (`id` and `id + 8192*k` share a slot
    /// index) cannot complete a *different* in-flight request. On any
    /// failure the slot is left exactly as `complete`/`complete_nack` have
    /// always left it (WAITING restored on an id mismatch, untouched on a
    /// failed CAS), so this refactor changes no observable behavior.
    fn claim_slot_for_completion(&self, correlation_id: u32) -> Option<&PendingResponseSlot> {
        let slot_ref = &self.pending[Self::slot_index(correlation_id)];
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
            return None;
        }
        if slot_ref.id.load(Ordering::Relaxed) != correlation_id {
            slot_ref.state.store(SLOT_WAITING, Ordering::Release);
            return None;
        }
        Some(slot_ref)
    }

    /// Publish a claimed slot's outcome as READY and wake its waiter.
    fn publish_outcome(slot_ref: &PendingResponseSlot, outcome: CorrelationOutcome) {
        unsafe {
            (*slot_ref.response.get()).write(outcome);
        }
        slot_ref.state.store(SLOT_READY, Ordering::Release);
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
        let Some(slot_ref) = self.claim_slot_for_completion(correlation_id) else {
            return false;
        };
        let Some(response) = response.take() else {
            slot_ref.state.store(SLOT_EMPTY, Ordering::Release);
            slot_ref.waker.wake();
            return false;
        };
        Self::publish_outcome(slot_ref, CorrelationOutcome::Response(response));
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
        Self::publish_outcome(slot_ref, CorrelationOutcome::Nack(reason));
        true
    }

    /// Cancel a pending request (used when send fails).
    ///
    /// Mirrors [`complete`](Self::complete): the slot's stored full
    /// correlation id is verified before the slot is released. `id` and
    /// `id + 8192*k` alias to the same slot index, so a stale cancel for a
    /// recycled id must not evict (or drop the ready response of) a
    /// *different* in-flight request occupying the same slot. On a mismatch
    /// the previous state is restored and no waker fires.
    pub(crate) fn cancel(&self, correlation_id: u32) {
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
                            SLOT_WRITING,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        if slot_ref.id.load(Ordering::Relaxed) != correlation_id {
                            slot_ref.state.store(SLOT_WAITING, Ordering::Release);
                            return;
                        }
                        slot_ref.state.store(SLOT_EMPTY, Ordering::Release);
                        slot_ref.waker.wake();
                        return;
                    }
                }
                SLOT_READY => {
                    if slot_ref
                        .state
                        .compare_exchange(
                            SLOT_READY,
                            SLOT_WRITING,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        if slot_ref.id.load(Ordering::Relaxed) != correlation_id {
                            slot_ref.state.store(SLOT_READY, Ordering::Release);
                            return;
                        }
                        unsafe {
                            (*slot_ref.response.get()).assume_init_drop();
                        }
                        slot_ref.state.store(SLOT_EMPTY, Ordering::Release);
                        slot_ref.waker.wake();
                        return;
                    }
                }
                SLOT_WRITING => {
                    std::hint::spin_loop();
                }
                _ => return,
            }
        }
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
        correlation_id: u32,
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
                // R-10: enter WRITING and verify the full id before evicting, so a
                // timeout cannot evict an innocent aliased WAITING request that
                // recycled this slot after cancel_all. On id mismatch restore
                // WAITING and fall through (the slot belongs to a different
                // request now).
                if slot_ref
                    .state
                    .compare_exchange(
                        SLOT_WAITING,
                        SLOT_WRITING,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    if slot_ref.id.load(Ordering::Acquire) != correlation_id {
                        slot_ref.state.store(SLOT_WAITING, Ordering::Release);
                    } else {
                        slot_ref.state.store(SLOT_EMPTY, Ordering::Release);
                        return Err(crate::GossipError::Timeout);
                    }
                }
                match Self::try_take_ready(slot_ref, correlation_id) {
                    ReadyTake::Taken(outcome) => return outcome.into_result(),
                    ReadyTake::ForeignReady => return Err(crate::GossipError::ConnectionDropped),
                    ReadyTake::NotReady => {}
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
        correlation_id: u32,
    ) -> Result<crate::AlignedBytes> {
        let slot = Self::slot_index(correlation_id);
        let slot_ref = &self.pending[slot];

        futures::future::poll_fn(|cx| {
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
            ReadyTake::Taken(CorrelationOutcome::Response(response)) => response,
            ReadyTake::Taken(CorrelationOutcome::Nack(reason)) => {
                panic!("{msg}: slot held a NACK ({reason}), not a response")
            }
            ReadyTake::NotReady => panic!("{msg}: slot was not READY (NotReady)"),
            ReadyTake::ForeignReady => panic!("{msg}: slot held a foreign id (ForeignReady)"),
        }
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
        let mut response = Some(crate::AlignedBytes::from_pooled_slice(b"reply", pool));
        assert!(tracker.complete(id, &mut response));
        assert!(response.is_none());

        drop(guard);
        let slot = CorrelationTracker::slot_index(id);
        assert_eq!(
            tracker.pending[slot].state.load(Ordering::Acquire),
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
        let mut response = Some(crate::AlignedBytes::from_pooled_slice(b"reply", pool));
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
        let mut response = Some(crate::AlignedBytes::from_pooled_slice(b"reply", pool));
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
        let mut response = Some(crate::AlignedBytes::from_pooled_slice(b"reply", pool));
        assert!(tracker.complete(id, &mut response));

        let taken = CorrelationTracker::try_take_ready_before_release(slot_ref, id, || {
            assert_eq!(
                slot_ref.state.load(Ordering::Acquire),
                SLOT_WRITING,
                "the slot must not be reusable while its response is being read"
            );
        });

        assert_eq!(expect_taken(taken, "ready response").as_ref(), b"reply");
        assert_eq!(slot_ref.state.load(Ordering::Acquire), SLOT_EMPTY);
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
        let mut response = Some(crate::AlignedBytes::from_pooled_slice(b"reply", pool));
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
            slot_ref.state.load(Ordering::Acquire),
            SLOT_WAITING,
            "aliased WAITING entry must survive a stale waiter (R-10)"
        );
        assert_eq!(
            slot_ref.id.load(Ordering::Acquire),
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
        assert_eq!(CorrelationTracker::slot_index(id_y), slot, "Y must alias to S");
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
        let mut response = Some(crate::AlignedBytes::from_pooled_slice(b"for-Y", pool));
        assert!(tracker.complete(id_y, &mut response));
        assert_eq!(y_wakes.load(Ordering::SeqCst), 1, "complete(Y) must wake Y's waker");
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
        let y_wait = tokio::spawn(async move {
            tracker_for_wait.wait_for_response_no_timeout(id_y).await
        });
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
        let mut response = Some(crate::AlignedBytes::from_pooled_slice(b"for-Y", pool));
        assert!(tracker.complete(id_y, &mut response));

        // Y must receive its reply promptly -- the no-timeout path must not hang.
        let reply = tokio::time::timeout(std::time::Duration::from_secs(2), y_wait)
            .await
            .expect("Y's no-timeout wait must not hang (R-10)")
            .expect("Y's task joined");
        assert_eq!(reply.unwrap().as_ref(), b"for-Y");
        guard_y.disarm();
    }
}
