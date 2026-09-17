use arc_swap::ArcSwapOption;
use crossbeam_queue::ArrayQueue;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use tokio::sync::{Notify, OwnedSemaphorePermit};

use crate::ReplyPayload;

pub(crate) const REPLY_SLOT_CAP: usize = 64;

const STATE_RESERVED: u8 = 0;
const STATE_COMPLETE: u8 = 2;

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
    max_reply_bytes: usize,
    terminal_payload: Arc<ReplyPayload>,
    normal_payload: std::sync::OnceLock<ReplyPayload>,
    cancelled: AtomicBool,
    activated: AtomicBool,
    state: AtomicU8,
    completion: Notify,
    // The record owns these permits for its entire transport lifetime. Keeping
    // them as fields avoids a release lock and makes writer-owned retention
    // part of the same bounded record.
    _job_permit: OwnedSemaphorePermit,
    _byte_permit: OwnedSemaphorePermit,
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
        self.normal_payload.set(payload).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "reply lease already has a submitted response",
            )
            .into()
        })
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
        self.cancelled.store(true, Ordering::Release);
        self.activate();
    }

    pub(crate) fn activate(&self) {
        if self.activated.swap(true, Ordering::AcqRel) {
            return;
        }
        let Some(slots) = self.slots.upgrade() else {
            self.complete();
            return;
        };
        if slots.closed.load(Ordering::Acquire) {
            self.complete();
            return;
        }
        // Every active record owns a distinct slot, and the ready queue has
        // exactly the same capacity. A push failure is therefore only possible
        // when the IO task has already closed the connection.
        let Some(record) = self.self_ref.upgrade() else {
            self.complete();
            return;
        };
        if slots.ready.push(record).is_err() {
            self.complete();
            slots.finish(self.index, self.generation);
            return;
        }
        slots.ready_notify.notify_one();
    }

    pub(crate) async fn wait_complete(&self) -> crate::Result<()> {
        loop {
            let notified = self.completion.notified();
            if self.state.load(Ordering::Acquire) == STATE_COMPLETE {
                return Ok(());
            }
            notified.await;
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
            self.completion.notify_waiters();
        }
    }
}

impl Drop for ReplySlotRecord {
    fn drop(&mut self) {
        if let Some(slots) = self.slots.upgrade() {
            slots.recycle(self.index, self.generation);
        }
    }
}

pub(crate) struct ReplySlots {
    instance_id: u64,
    addr: SocketAddr,
    pub(crate) ready: ArrayQueue<Arc<ReplySlotRecord>>,
    ready_notify: Arc<Notify>,
    free: ArrayQueue<usize>,
    generations: Box<[AtomicU64]>,
    table: Box<[ArcSwapOption<ReplySlotRecord>]>,
    closed: AtomicBool,
}

impl ReplySlots {
    pub(crate) fn new(instance_id: u64, addr: SocketAddr, ready_notify: Arc<Notify>) -> Arc<Self> {
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
            ready_notify,
            free,
            generations,
            table,
            closed: AtomicBool::new(false),
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
        if self.closed.load(Ordering::Acquire) {
            return Err(crate::GossipError::ConnectionClosed(self.addr));
        }
        let Some(index) = self.free.pop() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "reply slots are exhausted",
            )
            .into());
        };
        let Some(generation) = self.next_generation(index) else {
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
            max_reply_bytes,
            terminal_payload,
            normal_payload: std::sync::OnceLock::new(),
            cancelled: AtomicBool::new(false),
            activated: AtomicBool::new(false),
            state: AtomicU8::new(STATE_RESERVED),
            completion: Notify::new(),
            _job_permit: job_permit,
            _byte_permit: byte_permit,
        });
        self.table[index].store(Some(Arc::clone(&record)));
        Ok(record)
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

    pub(crate) fn notify(&self) -> Arc<Notify> {
        Arc::clone(&self.ready_notify)
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
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        while let Some(record) = self.ready.pop() {
            self.finish(record.index, record.generation);
        }
        for index in 0..REPLY_SLOT_CAP {
            if let Some(record) = self.table[index].swap(None) {
                record.complete();
            }
        }
        self.ready_notify.notify_waiters();
    }

    #[cfg(test)]
    pub(crate) fn reserved(&self) -> usize {
        (0..REPLY_SLOT_CAP)
            .filter(|index| self.table[*index].load().is_some())
            .count()
    }
}

#[cfg(test)]
mod tests;
