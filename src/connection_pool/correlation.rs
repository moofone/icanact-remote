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
    /// Full 32-bit correlation id of the request currently occupying this
    /// slot (meaningful whenever `state != SLOT_EMPTY`). Because `id` and
    /// `id + 8192*k` alias to the same slot index, the 13-bit slot index is
    /// not by itself a sufficient match — `complete()` verifies this full id
    /// so a stale/delayed response for a recycled id cannot complete a
    /// *different* in-flight request occupying the same slot.
    id: AtomicU32,
    response: UnsafeCell<MaybeUninit<crate::AlignedBytes>>,
    waker: AtomicWaker,
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
    ) -> Option<crate::AlignedBytes> {
        Self::try_take_ready_before_release(slot_ref, correlation_id, || {})
    }

    #[inline]
    fn try_take_ready_before_release(
        slot_ref: &PendingResponseSlot,
        correlation_id: u32,
        before_slot_release: impl FnOnce(),
    ) -> Option<crate::AlignedBytes> {
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
            // waiter's slot can be READY with a *different* request's response.
            // Without this check the waiter steals that response (and the
            // rightful owner later gets ConnectionDropped). On mismatch restore
            // READY and leave the response for the genuine owner.
            if slot_ref.id.load(Ordering::Relaxed) != correlation_id {
                slot_ref.state.store(SLOT_READY, Ordering::Release);
                return None;
            }
            // SAFETY: READY -> WRITING gives this reader exclusive ownership;
            // allocation requires EMPTY and cancellation spins on WRITING.
            let response = unsafe { (*slot_ref.response.get()).assume_init_read() };
            before_slot_release();
            slot_ref.state.store(SLOT_EMPTY, Ordering::Release);
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
            id: AtomicU32::new(0),
            response: UnsafeCell::new(MaybeUninit::uninit()),
            waker: AtomicWaker::new(),
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
                slot_ref.id.store(id, Ordering::Relaxed);
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

    /// Complete a pending request with a response.
    ///
    /// Returns true when the response was consumed and published.
    pub(crate) fn complete(
        &self,
        correlation_id: u32,
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

        // We now exclusively own the slot (WRITING). Reject a response whose
        // full correlation id does not match the request currently occupying
        // this slot: `id` and `id + 8192*k` share a slot index, so a stale or
        // delayed response for a recycled id must not complete a *different*
        // in-flight request. Restore the WAITING state so the genuine owner is
        // still completed by its own response.
        if slot_ref.id.load(Ordering::Relaxed) != correlation_id {
            slot_ref.state.store(SLOT_WAITING, Ordering::Release);
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
            if let Some(response) = Self::try_take_ready(slot_ref, correlation_id) {
                return std::task::Poll::Ready(Ok(response));
            }
            let state = slot_ref.state.load(Ordering::Acquire);
            if state == SLOT_EMPTY {
                return std::task::Poll::Ready(Err(crate::GossipError::ConnectionDropped));
            }

            slot_ref.waker.register(cx.waker());

            if let Some(response) = Self::try_take_ready(slot_ref, correlation_id) {
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
                    if slot_ref.id.load(Ordering::Relaxed) != correlation_id {
                        slot_ref.state.store(SLOT_WAITING, Ordering::Release);
                    } else {
                        slot_ref.state.store(SLOT_EMPTY, Ordering::Release);
                        return Err(crate::GossipError::Timeout);
                    }
                }
                if let Some(response) = Self::try_take_ready(slot_ref, correlation_id) {
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
        correlation_id: u32,
    ) -> Result<crate::AlignedBytes> {
        let slot = Self::slot_index(correlation_id);
        let slot_ref = &self.pending[slot];

        futures::future::poll_fn(|cx| {
            if let Some(response) = Self::try_take_ready(slot_ref, correlation_id) {
                return std::task::Poll::Ready(Ok(response));
            }
            let state = slot_ref.state.load(Ordering::Acquire);
            if state == SLOT_EMPTY {
                return std::task::Poll::Ready(Err(crate::GossipError::ConnectionDropped));
            }
            slot_ref.waker.register(cx.waker());
            if let Some(response) = Self::try_take_ready(slot_ref, correlation_id) {
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
        assert_eq!(taken.expect("ready response").as_ref(), b"reply");
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
            taken
                .expect("a stale cancel for an aliased correlation id dropped a ready response")
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

        assert_eq!(taken.expect("ready response").as_ref(), b"reply");
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
            CorrelationTracker::try_take_ready(slot_ref, stale_id).is_none(),
            "stale waiter must not steal a recycled slot's response (R-10)"
        );
        // The genuine owner still receives it.
        assert_eq!(
            CorrelationTracker::try_take_ready(slot_ref, id)
                .expect("genuine owner's response")
                .as_ref(),
            b"reply"
        );
        guard.disarm();
    }

    /// R-10: a stale waiter timing out on an aliased slot must NOT evict the
    /// different request currently WAITING there. Pre-fix the timeout path did
    /// a bare `WAITING -> EMPTY` CAS with no id check.
    #[tokio::test]
    async fn qa_r10_timeout_does_not_evict_an_aliased_waiter() {
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
            matches!(outcome, Err(crate::GossipError::Timeout)),
            "stale waiter should time out, got {outcome:?}"
        );
        let slot_ref = &tracker.pending[slot];
        assert_eq!(
            slot_ref.state.load(Ordering::Acquire),
            SLOT_WAITING,
            "aliased WAITING entry must survive a stale timeout (R-10)"
        );
        assert_eq!(
            slot_ref.id.load(Ordering::Relaxed),
            id,
            "the slot must still belong to id, not the stale waiter"
        );
        guard.disarm();
    }
}
