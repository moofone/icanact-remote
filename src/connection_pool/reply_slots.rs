use arc_swap::ArcSwapOption;
use crossbeam_queue::ArrayQueue;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use tokio::sync::{Notify, OwnedSemaphorePermit};

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
const PUBLICATION_OPEN: u8 = 0;
const PUBLICATION_CLOSED: u8 = 1;
const PUBLICATION_WRITING: u8 = 2;
const RESERVATION_CLOSED: usize = 1usize << (usize::BITS - 1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StreamKey {
    pub(crate) stream_id: u32,
    pub(crate) generation: u64,
}

pub(crate) struct ReplySlotRecord {
    slots: Weak<ReplySlots>,
    self_ref: Weak<ReplySlotRecord>,
    index: usize,
    generation: u64,
    correlation_id: u32,
    instance_id: u64,
    addr: SocketAddr,
    max_reply_bytes: usize,
    terminal_payload: Arc<ReplyPayload>,
    normal_payload: std::sync::OnceLock<ReplyPayload>,
    cancelled: AtomicBool,
    activated: AtomicBool,
    state: AtomicU8,
    #[cfg(any(test, feature = "test-helpers"))]
    /// Final outcome selected by the writer and committed on the wire. This
    /// deliberately does not infer outcome from payload/cancellation state.
    wire_outcome: AtomicU8,
    /// Publication has its own tiny state machine so close and synchronous
    /// publication have one linearization point. `PUBLICATION_WRITING` is
    /// held only while storing the payload in `OnceLock`.
    publication_state: AtomicU8,
    completion: Notify,
    // The record owns these permits for its entire transport lifetime. Keeping
    // them as fields avoids a release lock and makes writer-owned retention
    // part of the same bounded record.
    _job_permit: OwnedSemaphorePermit,
    _byte_permit: OwnedSemaphorePermit,
    #[cfg(any(test, feature = "test-helpers"))]
    stats: Arc<crate::connection_pool::lease_stats::LeaseStats>,
}

impl ReplySlotRecord {
    pub(crate) fn correlation_id(&self) -> u32 {
        self.correlation_id
    }

    pub(crate) fn instance_id(&self) -> u64 {
        self.instance_id
    }

    pub(crate) fn key(&self) -> StreamKey {
        StreamKey {
            stream_id: self.index as u32,
            generation: self.generation,
        }
    }

    pub(crate) fn publish(&self, payload: ReplyPayload) -> crate::Result<()> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "reply lease was cancelled",
            )
            .into());
        }
        if payload.len() > self.max_reply_bytes {
            return Err(crate::GossipError::MessageTooLarge {
                size: payload.len(),
                max: self.max_reply_bytes,
            });
        }
        loop {
            match self.publication_state.load(Ordering::Acquire) {
                PUBLICATION_CLOSED => {
                    return Err(crate::GossipError::ConnectionClosed(self.addr));
                }
                PUBLICATION_OPEN => {
                    if self
                        .publication_state
                        .compare_exchange(
                            PUBLICATION_OPEN,
                            PUBLICATION_WRITING,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_err()
                    {
                        continue;
                    }
                    break;
                }
                PUBLICATION_WRITING => std::hint::spin_loop(),
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "invalid reply publication state",
                    )
                    .into());
                }
            }
        }
        if self.cancelled.load(Ordering::Acquire) {
            self.publication_state
                .store(PUBLICATION_OPEN, Ordering::Release);
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "reply lease was cancelled",
            )
            .into());
        }
        let result = self.normal_payload.set(payload).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "reply lease already has a submitted response",
            )
            .into()
        });
        if result.is_ok() {
            #[cfg(any(test, feature = "test-helpers"))]
            self.stats
                .retained_payload_owners
                .fetch_add(1, Ordering::Relaxed);
        }
        self.publication_state
            .store(PUBLICATION_OPEN, Ordering::Release);
        result
    }

    pub(crate) fn normal_payload(&self) -> Option<ReplyPayload> {
        self.normal_payload.get().cloned()
    }

    pub(crate) fn terminal_payload(&self) -> Arc<ReplyPayload> {
        Arc::clone(&self.terminal_payload)
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn cancel(&self) {
        let was_cancelled = self.cancelled.swap(true, Ordering::AcqRel);
        #[cfg(any(test, feature = "test-helpers"))]
        if !was_cancelled {
            self.stats
                .cancellation_publications
                .fetch_add(1, Ordering::Relaxed);
            if self.state.load(Ordering::Acquire) != STATE_RESERVED
                || self.wire_outcome.load(Ordering::Acquire) != WIRE_OUTCOME_UNSET
            {
                // Once the writer has selected and committed a wire outcome,
                // cancellation can no longer alter delivery, even though the
                // record remains reserved until its terminal flush.
                self.stats
                    .too_late_cancellations
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        self.activate();
    }

    pub(crate) fn activate(&self) {
        if self.activated.swap(true, Ordering::AcqRel) {
            // A cancellation can arrive after publication has already made
            // the record active. Wake the owner again so it rechecks the
            // record instead of sleeping through that state change.
            if let Some(slots) = self.slots.upgrade() {
                slots.lease_notify.notify_one();
            }
            return;
        }
        let Some(slots) = self.slots.upgrade() else {
            self.complete_closed();
            return;
        };
        // Activation publishes into the same bounded ready set that close
        // sweeps. Fence this publication so close cannot finish its sweep
        // between the closed check and the queue push.
        let publishing = slots.begin_reservation();
        if !publishing || slots.closed.load(Ordering::Acquire) {
            if publishing {
                slots.end_reservation();
            }
            slots.remove_closed(self.index, self.generation);
            return;
        }
        // Every active record owns a distinct slot, and the ready queue has
        // exactly the same capacity. A push failure is therefore only possible
        // when the IO task has already closed the connection.
        let Some(record) = self.self_ref.upgrade() else {
            slots.end_reservation();
            self.complete_closed();
            return;
        };
        let pushed = slots.ready.push(record).is_ok();
        slots.end_reservation();
        if !pushed {
            self.complete_closed();
            slots.finish(self.index, self.generation);
            return;
        }
        slots.lease_notify.notify_one();
    }

    pub(crate) async fn wait_complete(&self) -> crate::Result<()> {
        loop {
            let notified = self.completion.notified();
            match self.state.load(Ordering::Acquire) {
                STATE_COMPLETE => return Ok(()),
                STATE_COMPLETION_CLOSED => {
                    return Err(crate::GossipError::ConnectionClosed(self.addr));
                }
                _ => notified.await,
            }
        }
    }

    pub(crate) fn finish(&self) {
        let _ = self.key();
        if let Some(slots) = self.slots.upgrade() {
            slots.finish(self.index, self.generation);
        } else {
            self.complete();
        }
    }

    pub(crate) fn complete(&self) {
        if self
            .state
            .compare_exchange(
                STATE_RESERVED,
                STATE_COMPLETE,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            #[cfg(any(test, feature = "test-helpers"))]
            {
                self.stats.live_slots.fetch_sub(1, Ordering::Relaxed);
                if self.wire_outcome.load(Ordering::Acquire) == WIRE_OUTCOME_NORMAL {
                    self.stats
                        .normal_completions
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    // An unset outcome is conservative: connection teardown,
                    // cancellation, and terminal fallback are not successful
                    // normal wire completions.
                    self.stats
                        .terminal_completions
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            self.completion.notify_waiters();
        }
    }

    #[inline]
    pub(crate) fn note_normal_outcome(&self) {
        #[cfg(any(test, feature = "test-helpers"))]
        self.wire_outcome
            .store(WIRE_OUTCOME_NORMAL, Ordering::Release);
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

    fn close_publication(&self) {
        loop {
            match self.publication_state.load(Ordering::Acquire) {
                PUBLICATION_OPEN => {
                    if self
                        .publication_state
                        .compare_exchange(
                            PUBLICATION_OPEN,
                            PUBLICATION_CLOSED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return;
                    }
                }
                PUBLICATION_CLOSED => return,
                PUBLICATION_WRITING => std::hint::spin_loop(),
                _ => return,
            }
        }
    }

    fn complete_closed(&self) {
        self.close_publication();
        if self
            .state
            .compare_exchange(
                STATE_RESERVED,
                STATE_COMPLETION_CLOSED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            #[cfg(any(test, feature = "test-helpers"))]
            self.stats.live_slots.fetch_sub(1, Ordering::Relaxed);
            self.completion.notify_waiters();
        }
    }

    pub(crate) fn discard(&self) {
        if let Some(slots) = self.slots.upgrade() {
            slots.discard(self.index, self.generation);
        } else {
            self.complete_discarded();
        }
    }

    fn complete_discarded(&self) {
        if self
            .state
            .compare_exchange(
                STATE_RESERVED,
                STATE_COMPLETE,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            #[cfg(any(test, feature = "test-helpers"))]
            {
                self.stats.live_slots.fetch_sub(1, Ordering::Relaxed);
                self.stats
                    .discarded_reservations
                    .fetch_add(1, Ordering::Relaxed);
            }
            self.completion.notify_waiters();
        }
    }
}

impl Drop for ReplySlotRecord {
    fn drop(&mut self) {
        #[cfg(any(test, feature = "test-helpers"))]
        {
            self.stats.reserved_jobs.fetch_sub(1, Ordering::Relaxed);
            self.stats
                .reserved_bytes
                .fetch_sub(self.max_reply_bytes, Ordering::Relaxed);
            if self.normal_payload.get().is_some() {
                self.stats
                    .retained_payload_owners
                    .fetch_sub(1, Ordering::Relaxed);
            }
        }
        if let Some(slots) = self.slots.upgrade() {
            slots.recycle(self.index, self.generation);
        }
    }
}

pub(crate) struct ReplySlots {
    instance_id: u64,
    addr: SocketAddr,
    pub(crate) ready: ArrayQueue<Arc<ReplySlotRecord>>,
    /// Lease activation never shares this notifier with lifecycle waiters.
    lease_notify: Arc<Notify>,
    exit_notify: Arc<Notify>,
    free: ArrayQueue<usize>,
    generations: Box<[AtomicU64]>,
    table: Box<[ArcSwapOption<ReplySlotRecord>]>,
    closed: AtomicBool,
    // The closed bit fences `try_reserve` publishers. The low bits count
    // publishers that have passed the closed check but have not published
    // their table entry yet, allowing close to sweep without a publication
    // race.
    reservation_state: AtomicUsize,
    #[cfg(any(test, feature = "test-helpers"))]
    stats: Arc<crate::connection_pool::lease_stats::LeaseStats>,
}

impl ReplySlots {
    pub(crate) fn new(instance_id: u64, addr: SocketAddr, exit_notify: Arc<Notify>) -> Arc<Self> {
        let free = ArrayQueue::new(REPLY_SLOT_CAP);
        for index in 0..REPLY_SLOT_CAP {
            let _ = free.push(index);
        }
        let generations = (0..REPLY_SLOT_CAP)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let table = (0..REPLY_SLOT_CAP)
            .map(|_| ArcSwapOption::empty())
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
            closed: AtomicBool::new(false),
            reservation_state: AtomicUsize::new(0),
            #[cfg(any(test, feature = "test-helpers"))]
            stats: Arc::new(crate::connection_pool::lease_stats::LeaseStats::default()),
        })
    }

    pub(crate) fn try_reserve(
        self: &Arc<Self>,
        correlation_id: u32,
        max_reply_bytes: usize,
        terminal_payload: Arc<ReplyPayload>,
        job_permit: OwnedSemaphorePermit,
        byte_permit: OwnedSemaphorePermit,
    ) -> std::result::Result<Arc<ReplySlotRecord>, crate::GossipError> {
        if !self.begin_reservation() {
            return Err(crate::GossipError::ConnectionClosed(self.addr));
        }
        let Some(index) = self.free.pop() else {
            self.end_reservation();
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "reply slots are exhausted",
            )
            .into());
        };
        let Some(generation) = self.next_generation(index) else {
            let _ = self.free.push(index);
            self.end_reservation();
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "reply slot generation is exhausted",
            )
            .into());
        };
        let record = Arc::new_cyclic(|weak| ReplySlotRecord {
            slots: Arc::downgrade(self),
            self_ref: weak.clone(),
            index,
            generation,
            correlation_id,
            instance_id: self.instance_id,
            addr: self.addr,
            max_reply_bytes,
            terminal_payload,
            normal_payload: std::sync::OnceLock::new(),
            cancelled: AtomicBool::new(false),
            activated: AtomicBool::new(false),
            state: AtomicU8::new(STATE_RESERVED),
            #[cfg(any(test, feature = "test-helpers"))]
            wire_outcome: AtomicU8::new(WIRE_OUTCOME_UNSET),
            publication_state: AtomicU8::new(PUBLICATION_OPEN),
            completion: Notify::new(),
            _job_permit: job_permit,
            _byte_permit: byte_permit,
            #[cfg(any(test, feature = "test-helpers"))]
            stats: Arc::clone(&self.stats),
        });
        self.table[index].store(Some(Arc::clone(&record)));
        #[cfg(any(test, feature = "test-helpers"))]
        {
            self.stats.live_slots.fetch_add(1, Ordering::Relaxed);
            self.stats.reserved_jobs.fetch_add(1, Ordering::Relaxed);
            self.stats
                .reserved_bytes
                .fetch_add(max_reply_bytes, Ordering::Relaxed);
        }
        self.end_reservation();
        if self.closed.load(Ordering::Acquire) {
            self.remove_closed(index, generation);
            return Err(crate::GossipError::ConnectionClosed(self.addr));
        }
        Ok(record)
    }

    fn begin_reservation(&self) -> bool {
        loop {
            let state = self.reservation_state.load(Ordering::Acquire);
            if state & RESERVATION_CLOSED != 0 {
                return false;
            }
            if self
                .reservation_state
                .compare_exchange_weak(state, state + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }

    fn end_reservation(&self) {
        self.reservation_state.fetch_sub(1, Ordering::AcqRel);
    }

    fn next_generation(&self, index: usize) -> Option<u64> {
        loop {
            let current = self.generations[index].load(Ordering::Acquire);
            let next = current.checked_add(1)?;
            if self.generations[index]
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some(next);
            }
        }
    }

    pub(crate) fn pop_ready(&self) -> Option<Arc<ReplySlotRecord>> {
        self.ready.pop()
    }

    #[cfg(any(test, feature = "test-helpers"))]
    pub(crate) fn stats_snapshot(&self) -> crate::connection_pool::lease_stats::LeaseStatsSnapshot {
        self.stats.snapshot(self.free.len(), self.ready.len())
    }

    pub(crate) fn notify(&self) -> Arc<Notify> {
        Arc::clone(&self.lease_notify)
    }

    pub(crate) fn finish(&self, index: usize, generation: u64) {
        let Some(record) = self.table[index].load_full() else {
            return;
        };
        if record.generation == generation {
            let _ = self.table[index].compare_and_swap(&record, None);
            record.complete();
        }
    }

    fn discard(&self, index: usize, generation: u64) {
        let Some(record) = self.table[index].load_full() else {
            return;
        };
        if record.generation == generation {
            let _ = self.table[index].compare_and_swap(&record, None);
            record.complete_discarded();
        }
    }

    fn remove_closed(&self, index: usize, generation: u64) {
        let Some(record) = self.table[index].load_full() else {
            return;
        };
        if record.generation == generation {
            let _ = self.table[index].compare_and_swap(&record, None);
            record.complete_closed();
        }
    }

    fn recycle(&self, index: usize, generation: u64) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let Some(record) = self.table[index].load_full() else {
            let _ = self.free.push(index);
            return;
        };
        if record.generation == generation {
            return;
        }
        let _ = self.free.push(index);
    }

    pub(crate) fn close_and_reclaim(&self) {
        let previous = self
            .reservation_state
            .fetch_or(RESERVATION_CLOSED, Ordering::AcqRel);
        if previous & RESERVATION_CLOSED != 0 {
            return;
        }
        self.closed.store(true, Ordering::Release);
        while self.reservation_state.load(Ordering::Acquire) & !RESERVATION_CLOSED != 0 {
            std::hint::spin_loop();
        }
        while let Some(record) = self.ready.pop() {
            self.remove_closed(record.index, record.generation);
        }
        for index in 0..REPLY_SLOT_CAP {
            if let Some(record) = self.table[index].swap(None) {
                record.complete_closed();
            }
        }
        self.lease_notify.notify_waiters();
        self.exit_notify.notify_waiters();
    }

    #[cfg(test)]
    pub(crate) fn reserved(&self) -> usize {
        (0..REPLY_SLOT_CAP)
            .filter(|index| self.table[*index].load().is_some())
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

#[cfg(test)]
mod tests;
