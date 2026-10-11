#[cfg(test)]
use arc_swap::ArcSwapOption;
use crossbeam_queue::ArrayQueue;
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::Notify;
#[cfg(test)]
use tokio::sync::OwnedSemaphorePermit;

use crate::ReplyPayload;

pub(crate) const REPLY_SLOT_CAP: usize = 64;

const STATE_RESERVED: u8 = 0;
const STATE_COMPLETE: u8 = 2;
#[cfg(any(test, feature = "test-helpers"))]
const WIRE_OUTCOME_UNSET: u8 = 0;
#[cfg(any(test, feature = "test-helpers"))]
const WIRE_OUTCOME_NORMAL: u8 = 1;
#[cfg(any(test, feature = "test-helpers"))]
const WIRE_OUTCOME_TERMINAL: u8 = 2;
const STATE_COMPLETION_CLOSED: u8 = 1;
const COMPLETION_DISCARDED: u8 = 3;
const PUBLICATION_OPEN: u8 = 0;
const PUBLICATION_CLOSED: u8 = 1;
const PUBLICATION_WRITING: u8 = 2;
const PUBLICATION_CLOSE_REQUESTED: u8 = 3;
const RESERVATION_CLOSED: usize = 1usize << (usize::BITS - 1);

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StreamKey {
    pub(crate) stream_id: u32,
    pub(crate) generation: u64,
}

/// One publication and one winning consumer per immutable reservation.
/// Unlike ArrayQueue(1), this cell needs no separate heap allocation.
struct ReplyPublication {
    payload: UnsafeCell<Option<ReplyPayload>>,
    ready: AtomicBool,
}

// SAFETY: the reservation's publication state grants exactly one producer.
// Release publication precedes the sole consumer's AcqRel swap; only that
// consumer takes the value. The cell is never reused during this reservation.
unsafe impl Sync for ReplyPublication {}

impl ReplyPublication {
    fn new() -> Self {
        Self {
            payload: UnsafeCell::new(None),
            ready: AtomicBool::new(false),
        }
    }

    fn publish(&self, payload: ReplyPayload) {
        // SAFETY: normal_published elects one producer for the record lifetime.
        unsafe { *self.payload.get() = Some(payload) };
        self.ready.store(true, Ordering::Release);
    }

    fn pop(&self) -> Option<ReplyPayload> {
        if !self.ready.swap(false, Ordering::AcqRel) {
            return None;
        }
        // SAFETY: the true-to-false winner exclusively owns the published value.
        unsafe { (&mut *self.payload.get()).take() }
    }
}

// Connection storage is allocated once. A scoped caller, table membership and
// the single numeric writer token jointly pin one immutable generation.
const CELL_INITIALIZING: usize = 1usize << (usize::BITS - 1);
const CELL_ACTIVE: usize = 1usize << (usize::BITS - 2);
const CELL_TABLE: usize = 1usize << (usize::BITS - 3);
const CELL_FINALIZING: usize = 1usize << (usize::BITS - 4);
const CELL_WRITER: usize = 1usize << (usize::BITS - 5);
const CELL_REFERENCES: usize = CELL_WRITER - 1;
const REPLY_BUDGET_CAP: usize = 64;
const REPLY_BUDGET_BYTES_CAP: usize = 16 * 1024 * 1024;

struct ReplySlotCell {
    ownership: AtomicUsize,
    record: UnsafeCell<MaybeUninit<ReplySlotRecord>>,
    // One fixed notification lifetime per cell; scoped references prevent
    // generation reuse while any completion waiter still borrows this cell.
    completion: Notify,
    #[cfg(test)]
    publication_test_gate: Arc<crate::connection_pool::lease_test_support::PublicationGate>,
}

// SAFETY: INITIALIZING exclusively installs one record. TABLE, WRITER, or a
// scoped caller reference prevents destruction. Only the ACTIVE -> FINALIZING
// winner destroys it; reuse starts after destruction and free-index recycling.
unsafe impl Sync for ReplySlotCell {}

impl ReplySlotCell {
    fn acquire<'a>(&self, slots: &'a ReplySlots, index: usize) -> Option<ReplySlotLease<'a>> {
        let previous = self.ownership.fetch_add(1, Ordering::AcqRel);
        assert!(previous & CELL_REFERENCES != CELL_REFERENCES);
        if previous & CELL_TABLE == 0 {
            self.release(slots, index);
            return None;
        }
        // SAFETY: the acquired caller reference pins this initialized record.
        let generation = unsafe { (&*self.record.get()).assume_init_ref().generation };
        Some(ReplySlotLease {
            slots: ReplySlotOwner::Scoped(slots),
            index,
            generation,
        })
    }

    fn release(&self, slots: &ReplySlots, index: usize) {
        let previous = self.ownership.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous & CELL_REFERENCES != 0);
        if previous == (CELL_ACTIVE | 1) {
            self.try_finalize(slots, index);
        }
    }

    fn retire(&self, slots: &ReplySlots, index: usize) {
        let previous = self.ownership.fetch_and(!CELL_TABLE, Ordering::AcqRel);
        if previous == (CELL_ACTIVE | CELL_TABLE) {
            self.try_finalize(slots, index);
        }
    }

    fn try_finalize(&self, slots: &ReplySlots, index: usize) {
        if self
            .ownership
            .compare_exchange(
                CELL_ACTIVE,
                CELL_FINALIZING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return;
        }
        // SAFETY: table, writer and callers are gone. Rejected probes cannot
        // dereference a FINALIZING record; the index is not available for reuse.
        unsafe { (&mut *self.record.get()).assume_init_drop() };
        self.ownership.fetch_sub(CELL_FINALIZING, Ordering::AcqRel);
        if !slots.closed.load(Ordering::Acquire) {
            let recycled = slots.free.push(index);
            debug_assert!(recycled.is_ok(), "slot must recycle exactly once");
        }
    }
}

impl Drop for ReplySlotCell {
    fn drop(&mut self) {
        if *self.ownership.get_mut() & CELL_ACTIVE != 0 {
            // SAFETY: exclusive connection destruction excludes borrowed leases
            // and writer views. ReplySlots::drop has already reclaimed resources.
            unsafe { self.record.get_mut().assume_init_drop() };
        }
    }
}

enum ReplySlotOwner<'a> {
    Scoped(&'a ReplySlots),
    #[cfg(test)]
    Fixture(Arc<ReplySlots>),
}

impl Deref for ReplySlotOwner<'_> {
    type Target = ReplySlots;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Scoped(slots) => slots,
            #[cfg(test)]
            Self::Fixture(slots) => slots,
        }
    }
}

/// A caller borrow into fixed connection storage. Production callers cannot
/// clone it or escape its connection/session lifetime.
pub(crate) struct ReplySlotLease<'a> {
    slots: ReplySlotOwner<'a>,
    index: usize,
    generation: u64,
}

#[cfg(test)]
impl Clone for ReplySlotLease<'_> {
    fn clone(&self) -> Self {
        let cell = &self.slots.table[self.index];
        let previous = cell.ownership.fetch_add(1, Ordering::AcqRel);
        assert!(previous & CELL_REFERENCES != CELL_REFERENCES);
        debug_assert!(previous & CELL_ACTIVE != 0);
        let slots = match &self.slots {
            ReplySlotOwner::Scoped(slots) => ReplySlotOwner::Scoped(slots),
            ReplySlotOwner::Fixture(slots) => ReplySlotOwner::Fixture(Arc::clone(slots)),
        };
        Self {
            slots,
            index: self.index,
            generation: self.generation,
        }
    }
}

impl Deref for ReplySlotLease<'_> {
    type Target = ReplySlotRecord;

    fn deref(&self) -> &Self::Target {
        // SAFETY: this scoped caller reference excludes destruction and reuse.
        let record = unsafe { (&*self.slots.table[self.index].record.get()).assume_init_ref() };
        debug_assert_eq!(record.generation, self.generation);
        record
    }
}

impl ReplySlotLease<'_> {
    #[cfg(test)]
    pub(crate) fn publish(&self, payload: ReplyPayload) -> crate::Result<()> {
        self.publish_observed(payload, |_| {})
    }

    pub(crate) fn publish_observed(
        &self,
        payload: ReplyPayload,
        observe: impl FnOnce(&ReplyPayload),
    ) -> crate::Result<()> {
        ReplySlotRecord::publish_observed(self, &self.slots, payload, observe)
    }

    pub(crate) fn cancel(&self) {
        ReplySlotRecord::cancel(self, &self.slots);
    }

    pub(crate) fn activate(&self) {
        ReplySlotRecord::activate(self, &self.slots);
    }

    pub(crate) async fn wait_complete(&self) -> crate::Result<()> {
        ReplySlotRecord::wait_complete(self, &self.slots).await
    }

    #[cfg(test)]
    pub(crate) fn finish(&self) {
        self.request_completion(&self.slots, STATE_COMPLETE);
    }

    #[cfg(test)]
    pub(crate) fn complete(&self) {
        self.finish();
    }

    pub(crate) fn discard(&self) {
        self.request_completion(&self.slots, COMPLETION_DISCARDED);
    }
}

impl Drop for ReplySlotLease<'_> {
    fn drop(&mut self) {
        self.slots.table[self.index].release(&self.slots, self.index);
    }
}

/// The sole writer lifetime, moved through the preallocated ready queue.
/// Fields stay private so tokens cannot be fabricated or duplicated.
#[derive(Debug)]
pub(crate) struct ReplySlotWriterToken {
    owner_id: usize,
    instance_id: u64,
    index: usize,
    generation: u64,
}

pub(crate) struct ReplySlotWriterRef<'a> {
    slots: &'a ReplySlots,
    record: &'a ReplySlotRecord,
}

impl Deref for ReplySlotWriterRef<'_> {
    type Target = ReplySlotRecord;

    fn deref(&self) -> &Self::Target {
        self.record
    }
}

impl ReplySlotWriterRef<'_> {
    pub(crate) fn finish(&self) {
        self.record.request_completion(self.slots, STATE_COMPLETE);
    }

    pub(crate) fn terminal_payload(&self) -> Option<ReplyTerminalPayloadRef<'_>> {
        match &self.record.terminal_payload {
            ReplyTerminalPayload::Bound(index) => Some(ReplyTerminalPayloadRef::Bound(
                &self.slots.budgets[*index].get()?.cancelled_reply,
            )),
            #[cfg(test)]
            ReplyTerminalPayload::Fixture(payload) => {
                let payload = payload.load();
                payload.as_ref()?;
                Some(ReplyTerminalPayloadRef::Fixture(payload))
            }
        }
    }
}

pub(crate) enum ReplyTerminalPayloadRef<'a> {
    Bound(&'a ReplyPayload),
    #[cfg(test)]
    Fixture(arc_swap::Guard<Option<Arc<ReplyPayload>>>),
}

impl Deref for ReplyTerminalPayloadRef<'_> {
    type Target = ReplyPayload;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Bound(payload) => payload,
            #[cfg(test)]
            Self::Fixture(payload) => payload.as_ref().expect("selected terminal payload"),
        }
    }
}

/// Cold, sole-IO-owner authority. Writer views borrow this owner, so teardown
/// cannot recycle their records while a view is live. Declare it before all
/// staged writer payloads, ensuring cancellation drops those owners first.
pub(crate) struct ReplySlotWriterOwner {
    slots: Arc<ReplySlots>,
}

impl ReplySlotWriterOwner {
    pub(crate) fn record<'a>(
        &'a self,
        token: &'a ReplySlotWriterToken,
    ) -> Option<ReplySlotWriterRef<'a>> {
        self.slots.writer_record(token)
    }

    pub(crate) fn finish(&self, token: ReplySlotWriterToken) {
        self.slots.finish_writer(token);
    }
}

impl Drop for ReplySlotWriterOwner {
    fn drop(&mut self) {
        // The final reservation/close drainer owns any deferred scan. Teardown
        // never races that drainer for numeric tokens or waits for a publisher.
        self.slots.writer_exited.store(true, Ordering::Release);
        self.slots.close_and_reclaim();
        self.slots.finish_writer_exit();
    }
}

enum ReplySlotResources {
    Budget {
        index: usize,
    },
    #[cfg(test)]
    Fixture {
        job_permit: OwnedSemaphorePermit,
        byte_permit: OwnedSemaphorePermit,
    },
}

enum ReplyTerminalPayload {
    Bound(usize),
    #[cfg(test)]
    Fixture(ArcSwapOption<ReplyPayload>),
}

pub(crate) struct ReplySlotRecord {
    index: usize,
    generation: u64,
    correlation_id: u32,
    instance_id: u64,
    addr: SocketAddr,
    max_reply_bytes: usize,
    terminal_payload: ReplyTerminalPayload,
    writer_active: AtomicBool,
    normal_payload: ReplyPublication,
    normal_published: AtomicBool,
    cancelled: AtomicBool,
    activated: AtomicBool,
    state: AtomicU8,
    requested_completion: AtomicU8,
    #[cfg(any(test, feature = "test-helpers"))]
    /// Final outcome selected by the writer and committed on the wire. This
    /// deliberately does not infer outcome from payload/cancellation state.
    wire_outcome: AtomicU8,
    #[cfg(any(test, feature = "test-helpers"))]
    /// The cancellation/outcome race has one accounting linearization point.
    too_late_recorded: AtomicBool,
    /// Publication has its own tiny state machine so close and synchronous
    /// publication have one linearization point. `PUBLICATION_WRITING` is
    /// held only while storing the payload in the publication cell.
    publication_state: AtomicU8,
    #[cfg(test)]
    publication_test_gate: Arc<crate::connection_pool::lease_test_support::PublicationGate>,
    // The record owns these permits for its entire transport lifetime. They
    // are consumed exactly once by the resources_recycled atomic winner.
    // No other path reads or writes this cell after construction.
    resources: UnsafeCell<Option<ReplySlotResources>>,
    resources_recycled: AtomicBool,
    #[cfg(any(test, feature = "test-helpers"))]
    stats: Arc<crate::connection_pool::lease_stats::LeaseStats>,
}

// SAFETY: all shared fields are synchronized independently. The sole
// UnsafeCell accessor is recycle_resources, whose AcqRel swap elects exactly
// one consumer for the entire immutable record lifetime. Construction occurs
// before Arc publication; no references to the cell's contents escape.
unsafe impl Sync for ReplySlotRecord {}

impl ReplySlotRecord {
    pub(crate) fn correlation_id(&self) -> u32 {
        self.correlation_id
    }

    pub(crate) fn instance_id(&self) -> u64 {
        self.instance_id
    }

    #[cfg(test)]
    pub(crate) fn key(&self) -> StreamKey {
        StreamKey {
            stream_id: self.index as u32,
            generation: self.generation,
        }
    }

    fn publish_observed(
        &self,
        slots: &ReplySlots,
        payload: ReplyPayload,
        observe: impl FnOnce(&ReplyPayload),
    ) -> crate::Result<()> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe).into());
        }
        if payload.retained_bytes() > self.max_reply_bytes {
            return Err(crate::GossipError::MessageTooLarge {
                size: payload.retained_bytes(),
                max: self.max_reply_bytes,
            });
        }
        if let Err(state) = self.publication_state.compare_exchange(
            PUBLICATION_OPEN,
            PUBLICATION_WRITING,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            if matches!(state, PUBLICATION_CLOSED | PUBLICATION_CLOSE_REQUESTED) {
                return Err(crate::GossipError::ConnectionClosed(self.addr));
            }
            return Err(std::io::Error::from(std::io::ErrorKind::AlreadyExists).into());
        }
        struct PublicationFinish<'a>(&'a ReplySlotRecord, &'a ReplySlots);

        impl Drop for PublicationFinish<'_> {
            fn drop(&mut self) {
                self.0.finish_publication(self.1);
            }
        }

        let _publication = PublicationFinish(self, slots);
        // This local is declared after the cleanup guard: on rejection or an
        // observer unwind, its original owner drops before permits can recycle.
        let mut publishing_payload = Some(payload);
        if self.cancelled.load(Ordering::Acquire) {
            return Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe).into());
        }
        #[cfg(test)]
        self.publication_test_gate.publisher_transition();
        if self.normal_published.load(Ordering::Acquire) {
            return Err(std::io::Error::from(std::io::ErrorKind::AlreadyExists).into());
        }
        // Borrow the original owner while exclusive publication is held. Close
        // cannot release the reservation during the callback, and no Bytes/Arc
        // clone is needed even for an explicitly installed observer. Unwinding
        // hands publication cleanup back before the lease's cancellation path.
        observe(publishing_payload.as_ref().expect("publication owner"));
        self.normal_payload
            .publish(publishing_payload.take().expect("publication owner"));
        self.normal_published.store(true, Ordering::Release);
        #[cfg(any(test, feature = "test-helpers"))]
        self.stats
            .retained_payload_owners
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub(crate) fn normal_payload(&self) -> Option<ReplyPayload> {
        self.normal_payload.pop()
    }

    #[cfg(test)]
    pub(crate) fn take_terminal_payload(&self) -> Option<Arc<ReplyPayload>> {
        match &self.terminal_payload {
            ReplyTerminalPayload::Fixture(payload) => payload.swap(None),
            ReplyTerminalPayload::Bound(_) => None,
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn cancel(&self, slots: &ReplySlots) {
        let _was_cancelled = self.cancelled.swap(true, Ordering::AcqRel);
        #[cfg(any(test, feature = "test-helpers"))]
        if !_was_cancelled {
            self.stats
                .cancellation_publications
                .fetch_add(1, Ordering::Relaxed);
            if self.state.load(Ordering::Acquire) != STATE_RESERVED
                || self.wire_outcome.load(Ordering::Acquire) != WIRE_OUTCOME_UNSET
            {
                // Once the writer has selected and committed a wire outcome,
                // cancellation can no longer alter delivery, even though the
                // record remains reserved until its terminal flush.
                self.note_too_late_once();
            }
        }
        self.activate(slots);
    }

    #[cfg(any(test, feature = "test-helpers"))]
    #[inline]
    fn note_too_late_once(&self) {
        if !self.too_late_recorded.swap(true, Ordering::AcqRel) {
            self.stats
                .too_late_cancellations
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn activate(&self, slots: &ReplySlots) {
        if self.requested_completion.load(Ordering::Acquire) != STATE_RESERVED {
            return;
        }
        if self.activated.swap(true, Ordering::AcqRel) {
            slots.lease_notify.notify_one();
            return;
        }
        if !slots.begin_reservation() {
            self.request_completion(slots, STATE_COMPLETION_CLOSED);
            return;
        }
        if slots.closed.load(Ordering::Acquire) {
            slots.end_reservation();
            self.request_completion(slots, STATE_COMPLETION_CLOSED);
            return;
        }
        self.writer_active.store(true, Ordering::Release);
        slots.table[self.index]
            .ownership
            .fetch_or(CELL_WRITER, Ordering::AcqRel);
        let token = ReplySlotWriterToken {
            owner_id: slots as *const ReplySlots as usize,
            instance_id: self.instance_id,
            index: self.index,
            generation: self.generation,
        };
        if let Err(token) = slots.ready.push(token) {
            slots.finish_writer(token);
        }
        slots.end_reservation();
        slots.lease_notify.notify_one();
    }

    async fn wait_complete(&self, slots: &ReplySlots) -> crate::Result<()> {
        loop {
            let notified = slots.table[self.index].completion.notified();
            tokio::pin!(notified);
            // Register this waiter before observing state. `complete()` may
            // publish between the observation and the first await; enabling
            // the pinned future closes that complete-before-first-poll gap.
            notified.as_mut().enable();
            match self.state.load(Ordering::Acquire) {
                STATE_COMPLETE => return Ok(()),
                STATE_COMPLETION_CLOSED => {
                    return Err(crate::GossipError::ConnectionClosed(self.addr));
                }
                _ => notified.await,
            }
        }
    }

    fn request_completion(&self, slots: &ReplySlots, outcome: u8) {
        let _ = self.requested_completion.compare_exchange(
            STATE_RESERVED,
            outcome,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.finish_requested_completion(slots);
    }

    fn finish_requested_completion(&self, slots: &ReplySlots) {
        if !self.close_publication()
            || self.writer_active.load(Ordering::Acquire)
            || !self.recycle_resources(slots)
        {
            return;
        }
        let outcome = self.requested_completion.load(Ordering::Acquire);
        #[cfg(any(test, feature = "test-helpers"))]
        match outcome {
            STATE_COMPLETE => {
                if self.wire_outcome.load(Ordering::Acquire) == WIRE_OUTCOME_NORMAL {
                    self.stats
                        .normal_completions
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    self.stats
                        .terminal_completions
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            COMPLETION_DISCARDED => {
                self.stats
                    .discarded_reservations
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        let completed = if outcome == STATE_COMPLETION_CLOSED {
            STATE_COMPLETION_CLOSED
        } else {
            STATE_COMPLETE
        };
        // Only the actual cleanup winner may publish completion. Its payload
        // and permit destruction have finished before this release store.
        self.state.store(completed, Ordering::Release);
        slots.table[self.index].completion.notify_waiters();
    }

    #[inline]
    pub(crate) fn note_normal_outcome(&self) {
        #[cfg(any(test, feature = "test-helpers"))]
        {
            self.wire_outcome
                .store(WIRE_OUTCOME_NORMAL, Ordering::Release);
            // Cancellation may win the race before the writer resolves the
            // committed final frame. Account for that path here, exactly once;
            // `cancel` handles the opposite ordering.
            if self.cancelled.load(Ordering::Acquire) {
                self.note_too_late_once();
            }
        }
    }

    #[inline]
    pub(crate) fn note_terminal_outcome(&self) {
        #[cfg(any(test, feature = "test-helpers"))]
        self.wire_outcome
            .store(WIRE_OUTCOME_TERMINAL, Ordering::Release);
    }

    /// Record an abort only after its complete frame has reached the writer.
    #[inline]
    pub(crate) fn note_abort(&self) {
        #[cfg(any(test, feature = "test-helpers"))]
        {
            self.stats
                .actual_stream_aborts
                .fetch_add(1, Ordering::Relaxed);
            self.wire_outcome
                .store(WIRE_OUTCOME_TERMINAL, Ordering::Release);
        }
    }

    #[inline]
    pub(crate) fn note_frame_progress(&self, bytes: usize) {
        #[cfg(any(test, feature = "test-helpers"))]
        self.stats
            .frame_progress
            .fetch_add(bytes, Ordering::Relaxed);
        #[cfg(not(any(test, feature = "test-helpers")))]
        let _ = bytes;
    }

    #[inline]
    pub(crate) fn note_flush_progress(&self) {
        #[cfg(any(test, feature = "test-helpers"))]
        self.stats.flush_progress.fetch_add(1, Ordering::Relaxed);
    }

    fn finish_publication(&self, slots: &ReplySlots) {
        if self
            .publication_state
            .compare_exchange(
                PUBLICATION_WRITING,
                PUBLICATION_OPEN,
                Ordering::Release,
                Ordering::Acquire,
            )
            .is_err()
        {
            self.publication_state
                .store(PUBLICATION_CLOSED, Ordering::Release);
            self.finish_requested_completion(slots);
        }
    }

    /// Closing a preempted publisher transfers cleanup to that publisher.
    /// The closer never spins, and permits stay charged until payload release.
    fn close_publication(&self) -> bool {
        let previous = self
            .publication_state
            .fetch_or(PUBLICATION_CLOSED, Ordering::AcqRel);
        if previous == PUBLICATION_WRITING {
            #[cfg(test)]
            self.publication_test_gate.close_transition();
        }
        matches!(previous, PUBLICATION_OPEN | PUBLICATION_CLOSED)
    }

    #[cfg(test)]
    pub(crate) fn publication_test_gate(
        &self,
    ) -> Arc<crate::connection_pool::lease_test_support::PublicationGate> {
        Arc::clone(&self.publication_test_gate)
    }
}

impl ReplySlotRecord {
    fn recycle_resources(&self, slots: &ReplySlots) -> bool {
        if self.resources_recycled.swap(true, Ordering::AcqRel) {
            return false;
        }
        slots.table[self.index].retire(slots, self.index);
        // Payload references protect the reservation just like their record
        // does. Explicitly remove both transport-owned references first, so a
        // payload owner cannot outlive the permits, accounting, or slot.
        let payload = self.normal_payload.pop();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)));
        #[cfg(test)]
        if let ReplyTerminalPayload::Fixture(payload) = &self.terminal_payload {
            let payload = payload.swap(None);
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)));
        }
        // SAFETY: this caller won resources_recycled above; no other caller
        // can access the cell, including the record's eventual destructor.
        let resources = unsafe { (&mut *self.resources.get()).take() };
        #[cfg(test)]
        self.stats.permits_released.store(false, Ordering::Release);
        match resources {
            Some(ReplySlotResources::Budget { index }) => {
                if let Some(budget) = slots.budgets[index].get() {
                    budget.release(self.max_reply_bytes);
                }
            }
            #[cfg(test)]
            Some(ReplySlotResources::Fixture {
                job_permit,
                byte_permit,
            }) => {
                drop(job_permit);
                drop(byte_permit);
            }
            None => {}
        }
        #[cfg(test)]
        self.stats.permits_released.store(true, Ordering::Release);
        #[cfg(any(test, feature = "test-helpers"))]
        {
            #[cfg(test)]
            if !self.stats.permits_released.load(Ordering::Acquire) {
                self.stats
                    .accounting_before_permits
                    .store(true, Ordering::Release);
            }
            self.stats.live_slots.fetch_sub(1, Ordering::Relaxed);
            self.stats.reserved_jobs.fetch_sub(1, Ordering::Release);
            self.stats
                .reserved_bytes
                .fetch_sub(self.max_reply_bytes, Ordering::Release);
            if self.normal_published.load(Ordering::Acquire) {
                self.stats
                    .retained_payload_owners
                    .fetch_sub(1, Ordering::Release);
            }
        }
        true
    }
}

pub(crate) struct ReplySlots {
    instance_id: u64,
    addr: SocketAddr,
    pub(crate) ready: ArrayQueue<ReplySlotWriterToken>,
    /// Lease activation never shares this notifier with lifecycle waiters.
    lease_notify: Arc<Notify>,
    exit_notify: Arc<Notify>,
    free: ArrayQueue<usize>,
    generations: Box<[AtomicU64]>,
    table: Box<[ReplySlotCell]>,
    budgets: Box<[OnceLock<Arc<crate::reply_lease::ReplyBudgetState>>]>,
    free_budgets: ArrayQueue<usize>,
    available_budget_bytes: AtomicUsize,
    closed: AtomicBool,
    writer_owner_claimed: AtomicBool,
    close_drained: AtomicBool,
    close_done: AtomicBool,
    writer_exited: AtomicBool,
    writer_exit_drained: AtomicBool,
    // The closed bit fences `try_reserve` publishers. The low bits count
    // publishers that have passed the closed check but have not published
    // their table entry yet, allowing close to sweep without a publication
    // race.
    reservation_state: AtomicUsize,
    #[cfg(any(test, feature = "test-helpers"))]
    stats: Arc<crate::connection_pool::lease_stats::LeaseStats>,
    #[cfg(test)]
    lease_test_gate: Arc<crate::connection_pool::lease_test_support::LeaseWakeGate>,
}

impl ReplySlots {
    pub(crate) fn new(instance_id: u64, addr: SocketAddr, exit_notify: Arc<Notify>) -> Arc<Self> {
        let free = ArrayQueue::new(REPLY_SLOT_CAP);
        for index in 0..REPLY_SLOT_CAP {
            let _ = free.push(index);
        }
        let free_budgets = ArrayQueue::new(REPLY_BUDGET_CAP);
        let budgets = (0..REPLY_BUDGET_CAP)
            .map(|index| {
                let _ = free_budgets.push(index);
                OnceLock::new()
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let generations = (0..REPLY_SLOT_CAP)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let table = (0..REPLY_SLOT_CAP)
            .map(|_| ReplySlotCell {
                ownership: AtomicUsize::new(0),
                record: UnsafeCell::new(MaybeUninit::uninit()),
                completion: super::preallocated_notify(),
                #[cfg(test)]
                publication_test_gate: Arc::new(
                    crate::connection_pool::lease_test_support::PublicationGate::new(),
                ),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Arc::new(Self {
            instance_id,
            addr,
            ready: ArrayQueue::new(REPLY_SLOT_CAP),
            lease_notify: Arc::new(Notify::new()),
            exit_notify,
            free,
            generations,
            table,
            budgets,
            free_budgets,
            available_budget_bytes: AtomicUsize::new(REPLY_BUDGET_BYTES_CAP),
            closed: AtomicBool::new(false),
            writer_owner_claimed: AtomicBool::new(false),
            close_drained: AtomicBool::new(false),
            close_done: AtomicBool::new(false),
            writer_exited: AtomicBool::new(false),
            writer_exit_drained: AtomicBool::new(false),
            reservation_state: AtomicUsize::new(0),
            #[cfg(any(test, feature = "test-helpers"))]
            stats: Arc::new(crate::connection_pool::lease_stats::LeaseStats::default()),
            #[cfg(test)]
            lease_test_gate: Arc::new(
                crate::connection_pool::lease_test_support::LeaseWakeGate::new(),
            ),
        })
    }

    pub(crate) fn writer_owner(self: &Arc<Self>) -> crate::Result<ReplySlotWriterOwner> {
        self.writer_owner_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| crate::GossipError::WriteQueueFull)?;
        Ok(ReplySlotWriterOwner {
            slots: Arc::clone(self),
        })
    }

    pub(crate) fn bind_budget(
        &self,
        budget: Arc<crate::reply_lease::ReplyBudgetState>,
    ) -> crate::Result<usize> {
        if self.closed.load(Ordering::Acquire) {
            return Err(crate::GossipError::ConnectionClosed(self.addr));
        }
        let retained_bytes = budget.cancelled_reply.retained_bytes();
        let available = self.available_budget_bytes.load(Ordering::Acquire);
        let remaining = available
            .checked_sub(retained_bytes)
            .ok_or(crate::GossipError::WriteQueueFull)?;
        self.available_budget_bytes
            .compare_exchange(available, remaining, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| crate::GossipError::WriteQueueFull)?;
        let Some(index) = self.free_budgets.pop() else {
            self.available_budget_bytes
                .fetch_add(retained_bytes, Ordering::Release);
            return Err(crate::GossipError::WriteQueueFull);
        };
        // This connection retains at most 64 registrations and 16 MiB of their
        // complete terminal owners. Shared duplicate registrations are charged
        // conservatively. Each index is assigned once for the connection lifetime.
        if self.budgets[index].set(budget).is_err() {
            self.available_budget_bytes
                .fetch_add(retained_bytes, Ordering::Release);
            return Err(crate::GossipError::WriteQueueFull);
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(crate::GossipError::ConnectionClosed(self.addr));
        }
        Ok(index)
    }

    pub(crate) fn budget(&self, index: usize) -> &crate::reply_lease::ReplyBudgetState {
        self.budgets[index].get().expect("bound reply budget")
    }

    pub(crate) fn try_reserve_bound(
        &self,
        correlation_id: u32,
        max_reply_bytes: usize,
        budget_index: usize,
    ) -> crate::Result<ReplySlotLease<'_>> {
        let budget = self.budget(budget_index);
        budget.try_reserve(max_reply_bytes)?;
        let result = self.reserve_record(
            correlation_id,
            max_reply_bytes,
            ReplyTerminalPayload::Bound(budget_index),
            ReplySlotResources::Budget {
                index: budget_index,
            },
        );
        match result {
            Ok((index, generation)) => {
                let record = ReplySlotLease {
                    slots: ReplySlotOwner::Scoped(self),
                    index,
                    generation,
                };
                if self.closed.load(Ordering::Acquire) {
                    record.request_completion(self, STATE_COMPLETION_CLOSED);
                    return Err(crate::GossipError::ConnectionClosed(self.addr));
                }
                Ok(record)
            }
            Err(error) => {
                budget.release(max_reply_bytes);
                Err(error)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn try_reserve(
        self: &Arc<Self>,
        correlation_id: u32,
        max_reply_bytes: usize,
        terminal_payload: Arc<ReplyPayload>,
        job_permit: OwnedSemaphorePermit,
        byte_permit: OwnedSemaphorePermit,
    ) -> crate::Result<ReplySlotLease<'static>> {
        let terminal = ArcSwapOption::from(Some(terminal_payload));
        let (index, generation) = self.reserve_record(
            correlation_id,
            max_reply_bytes,
            ReplyTerminalPayload::Fixture(terminal),
            ReplySlotResources::Fixture {
                job_permit,
                byte_permit,
            },
        )?;
        let record = ReplySlotLease {
            slots: ReplySlotOwner::Fixture(Arc::clone(self)),
            index,
            generation,
        };
        if self.closed.load(Ordering::Acquire) {
            record.request_completion(self, STATE_COMPLETION_CLOSED);
            return Err(crate::GossipError::ConnectionClosed(self.addr));
        }
        Ok(record)
    }

    fn reserve_record(
        &self,
        correlation_id: u32,
        max_reply_bytes: usize,
        terminal_payload: ReplyTerminalPayload,
        resources: ReplySlotResources,
    ) -> crate::Result<(usize, u64)> {
        if !self.begin_reservation() {
            return Err(crate::GossipError::ConnectionClosed(self.addr));
        }
        let Some(index) = self.free.pop() else {
            self.end_reservation();
            return Err(crate::GossipError::WriteQueueFull);
        };
        let Some(generation) = self.next_generation(index) else {
            // Permanent non-reissue: exhausted indices never re-enter free.
            self.end_reservation();
            return Err(crate::GossipError::WriteQueueFull);
        };
        let cell = &self.table[index];
        if cell
            .ownership
            .compare_exchange(0, CELL_INITIALIZING, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            let _ = self.free.push(index);
            self.end_reservation();
            return Err(crate::GossipError::WriteQueueFull);
        }
        let record = ReplySlotRecord {
            index,
            generation,
            correlation_id,
            instance_id: self.instance_id,
            addr: self.addr,
            max_reply_bytes,
            terminal_payload,
            writer_active: AtomicBool::new(false),
            normal_payload: ReplyPublication::new(),
            normal_published: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            activated: AtomicBool::new(false),
            state: AtomicU8::new(STATE_RESERVED),
            requested_completion: AtomicU8::new(STATE_RESERVED),
            #[cfg(any(test, feature = "test-helpers"))]
            wire_outcome: AtomicU8::new(WIRE_OUTCOME_UNSET),
            #[cfg(any(test, feature = "test-helpers"))]
            too_late_recorded: AtomicBool::new(false),
            publication_state: AtomicU8::new(PUBLICATION_OPEN),
            #[cfg(test)]
            publication_test_gate: Arc::clone(&cell.publication_test_gate),
            resources: UnsafeCell::new(Some(resources)),
            resources_recycled: AtomicBool::new(false),
            #[cfg(any(test, feature = "test-helpers"))]
            stats: Arc::clone(&self.stats),
        };
        // SAFETY: FREE -> INITIALIZING exclusively owns this storage.
        unsafe { std::ptr::write(cell.record.get(), MaybeUninit::new(record)) };
        cell.ownership.fetch_add(
            (CELL_ACTIVE | CELL_TABLE | 1).wrapping_sub(CELL_INITIALIZING),
            Ordering::Release,
        );
        #[cfg(any(test, feature = "test-helpers"))]
        {
            self.stats.live_slots.fetch_add(1, Ordering::Relaxed);
            self.stats.reserved_jobs.fetch_add(1, Ordering::Relaxed);
            self.stats
                .reserved_bytes
                .fetch_add(max_reply_bytes, Ordering::Relaxed);
        }
        self.end_reservation();
        // The initial caller reference pins storage until the caller constructs
        // its scoped lease. Close can complete but cannot destroy this record.
        Ok((index, generation))
    }

    fn begin_reservation(&self) -> bool {
        let state = self.reservation_state.fetch_add(1, Ordering::AcqRel);
        assert!(state & !RESERVATION_CLOSED != !RESERVATION_CLOSED);
        if state & RESERVATION_CLOSED != 0 {
            self.end_reservation();
            return false;
        }
        true
    }

    fn end_reservation(&self) {
        let previous = self.reservation_state.fetch_sub(1, Ordering::AcqRel);
        if previous == (RESERVATION_CLOSED | 1) {
            self.finish_close();
        }
    }

    fn next_generation(&self, index: usize) -> Option<u64> {
        // The free-index queue gives this reservation exclusive generation
        // ownership until recycle; there is no competing generation writer.
        let current = self.generations[index].load(Ordering::Acquire);
        let next = current.checked_add(1)?;
        self.generations[index].store(next, Ordering::Release);
        Some(next)
    }

    pub(crate) fn pop_ready(&self) -> Option<ReplySlotWriterToken> {
        self.ready.pop()
    }

    #[cfg(any(test, feature = "test-helpers"))]
    pub(crate) fn stats_snapshot(&self) -> crate::connection_pool::lease_stats::LeaseStatsSnapshot {
        self.stats.snapshot(self.free.len(), self.ready.len())
    }

    pub(crate) fn notify(&self) -> Arc<Notify> {
        Arc::clone(&self.lease_notify)
    }

    #[cfg(test)]
    pub(crate) fn lease_test_gate(
        &self,
    ) -> Arc<crate::connection_pool::lease_test_support::LeaseWakeGate> {
        Arc::clone(&self.lease_test_gate)
    }

    fn writer_record<'a>(
        &'a self,
        token: &'a ReplySlotWriterToken,
    ) -> Option<ReplySlotWriterRef<'a>> {
        if token.owner_id != self as *const ReplySlots as usize
            || token.instance_id != self.instance_id
        {
            return None;
        }
        let cell = self.table.get(token.index)?;
        if cell.ownership.load(Ordering::Acquire) & CELL_WRITER == 0 {
            return None;
        }
        // SAFETY: the unique borrowed writer token owns WRITER until consumed by
        // finish_writer. That immutable generation cannot be recycled meanwhile.
        let record = unsafe { (&*cell.record.get()).assume_init_ref() };
        if record.generation != token.generation {
            return None;
        }
        Some(ReplySlotWriterRef {
            slots: self,
            record,
        })
    }

    /// Consume writer ownership only after its staged payloads have been dropped.
    fn finish_writer(&self, token: ReplySlotWriterToken) {
        let Some(record) = self.writer_record(&token) else {
            return;
        };
        if record.requested_completion.load(Ordering::Acquire) == STATE_RESERVED {
            record.request_completion(self, STATE_COMPLETION_CLOSED);
        }
        record.writer_active.store(false, Ordering::Release);
        record.finish_requested_completion(self);
        let cell = &self.table[token.index];
        let previous = cell.ownership.fetch_and(!CELL_WRITER, Ordering::AcqRel);
        if previous == (CELL_ACTIVE | CELL_WRITER) {
            cell.try_finalize(self, token.index);
        }
    }

    pub(crate) fn close_and_reclaim(&self) {
        self.closed.store(true, Ordering::Release);
        let previous = self
            .reservation_state
            .fetch_or(RESERVATION_CLOSED, Ordering::AcqRel);
        if previous & RESERVATION_CLOSED != 0 {
            return;
        }
        if previous == 0 {
            self.finish_close();
        }
        // Otherwise the final active reservation publisher owns the drain.
        // Its permits remain charged; teardown never spins waiting for it.
    }

    fn finish_close(&self) {
        if self.close_drained.swap(true, Ordering::AcqRel) {
            return;
        }
        while let Some(token) = self.ready.pop() {
            self.finish_writer(token);
        }
        for index in 0..REPLY_SLOT_CAP {
            if let Some(record) = self.table[index].acquire(self, index) {
                record.request_completion(self, STATE_COMPLETION_CLOSED);
            }
        }
        self.lease_notify.notify_waiters();
        self.exit_notify.notify_waiters();
        self.close_done.store(true, Ordering::Release);
        self.finish_writer_exit();
    }

    fn finish_writer_exit(&self) {
        if !self.writer_exited.load(Ordering::Acquire)
            || !self.close_done.load(Ordering::Acquire)
            || self.writer_exit_drained.swap(true, Ordering::AcqRel)
        {
            return;
        }
        // Ready draining is finished, all admission/activation publishers have
        // exited, and the unique IO owner can no longer lend writer views. This
        // sole cold scan owns all abandoned writer bits; no other path can race
        // it to remove them or recycle their records.
        for index in 0..self.table.len() {
            let cell = &self.table[index];
            if cell.ownership.load(Ordering::Acquire) & CELL_WRITER == 0 {
                continue;
            }
            // SAFETY: WRITER pins initialization and this is its sole reclaimer.
            let generation = unsafe { (&*cell.record.get()).assume_init_ref().generation };
            self.finish_writer(ReplySlotWriterToken {
                owner_id: self as *const ReplySlots as usize,
                instance_id: self.instance_id,
                index,
                generation,
            });
        }
    }

    #[cfg(test)]
    pub(crate) fn reserved(&self) -> usize {
        (0..REPLY_SLOT_CAP)
            .filter(|index| self.table[*index].ownership.load(Ordering::Acquire) & CELL_TABLE != 0)
            .count()
    }

    #[cfg(test)]
    pub(crate) fn force_generation_exhaustion(&self, index: usize) {
        self.generations[index].store(u64::MAX, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn free_count(&self) -> usize {
        self.free.len()
    }
}

impl Drop for ReplySlots {
    fn drop(&mut self) {
        // No production caller borrow or writer view can coexist with exclusive
        // connection destruction. Destroy remaining payloads before budget owners.
        for index in 0..self.table.len() {
            let cell = &self.table[index];
            if cell.ownership.load(Ordering::Relaxed) & CELL_ACTIVE == 0 {
                continue;
            }
            // SAFETY: ACTIVE denotes initialized storage; &mut self excludes use.
            let record = unsafe { (&*cell.record.get()).assume_init_ref() };
            record.writer_active.store(false, Ordering::Relaxed);
            // Keep TABLE set until the cell's destructor; no free-index recycling
            // or record destruction may occur while this cleanup borrows it.
            cell.ownership.fetch_add(1, Ordering::Relaxed);
            record.recycle_resources(self);
        }
    }
}

#[cfg(test)]
mod tests;
