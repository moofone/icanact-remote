//! Fixed, physical-stream terminal ownership. Application cancellation never
//! returns a wire credit: only a complete terminal frame on this same stream
//! does. The receiver independently enforces the advertised 64-outcome bound.
use crate::framing::{self, WireKind};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub(crate) const OUTCOME_CAP: usize = 64;

/// Fixed physical-stream metadata budgets: 4096 data records, 128 ordinary
/// control records, and 128 terminal records. Neither data nor gossip/route
/// traffic may borrow terminal capacity. Each record is five atomic u64s;
/// slab bytes are exactly 4352 * size_of::<FrameCell>(), with three bounded
/// free-index queues of these same capacities. There is no allocation fallback.
///
/// Typed fixed headers, generic frames and vectored producer commands all
/// transfer the original record; IO preparation never classifies them again.
/// Records live until transfer into the IO owner's fixed 64-frame cache,
/// committed shared-stream bytes, admission rollback, or physical teardown.
/// Exhaustion returns WriteQueueFull before publishing that command; a
/// partially published chunked send retains its existing fail-closed contract.
/// No deadline task, per-message timer, reference-count owner, or payload copy
/// is added. All backing storage is allocated once at physical-stream startup.
const FRAME_RECEIPT_CAP: usize = 4096;
const CONTROL_FRAME_RECEIPT_CAP: usize = 128;
const TERMINAL_FRAME_RECEIPT_CAP: usize = 128;
// Ordinary replies use the common finite data inventory, not the reserved
// terminal lane. This keeps the existing 4352-record bound and prevents a
// full ordinary response queue from stealing critical delivery receipts.
const FRAME_INDEX_BITS: u32 = 13;
const FRAME_LINKING: u64 = 1 << 63;
const FRAME_RELEASE_REQUESTED: u64 = 1 << 62;
static NEXT_FRAME_STORE_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FrameToken {
    index: usize,
    generation: u64,
}

#[derive(Debug, Default)]
pub(super) struct AdmittedFrames {
    owner: u64,
    first: Option<FrameToken>,
    last: Option<FrameToken>,
}

impl AdmittedFrames {
    pub(super) fn is_empty(&self) -> bool {
        self.first.is_none()
    }
}

/// Rollback-only authority retained by a producer while publication can
/// still fail. It cannot be handed to a writer or used to append a chain.
struct FrameRollback {
    owner: u64,
    first: Option<FrameToken>,
}

#[derive(Clone, Copy, Debug)]
struct StoredFrame {
    frame: Frame,
    offset: usize,
    bytes: usize,
    next: Option<FrameToken>,
}

#[derive(Debug)]
struct FrameCell {
    generation: AtomicU64,
    control_a: AtomicU64,
    b_c: AtomicU64,
    range: AtomicU64,
    next: AtomicU64,
}

impl FrameToken {
    fn encoded(self) -> u64 {
        (self.generation << FRAME_INDEX_BITS) | self.index as u64
    }

    fn decode(value: u64) -> Option<Self> {
        (value != 0).then_some(Self {
            index: value as usize & ((1 << FRAME_INDEX_BITS) - 1),
            generation: value >> FRAME_INDEX_BITS,
        })
    }
}

#[derive(Debug)]
struct FrameStore {
    owner: u64,
    data_capacity: usize,
    closed: std::sync::atomic::AtomicBool,
    cells: Box<[FrameCell]>,
    free: crossbeam_queue::ArrayQueue<usize>,
    control_free: crossbeam_queue::ArrayQueue<usize>,
    terminal_free: crossbeam_queue::ArrayQueue<usize>,
    #[cfg(test)]
    decoded_headers: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    transferred_records: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    byte_observations: std::sync::atomic::AtomicUsize,
}

impl FrameStore {
    fn new(capacity: usize) -> Self {
        let control_capacity = if capacity == 0 {
            0
        } else {
            CONTROL_FRAME_RECEIPT_CAP
        };
        let terminal_capacity = if capacity == 0 {
            0
        } else {
            TERMINAL_FRAME_RECEIPT_CAP
        };
        let free = crossbeam_queue::ArrayQueue::new(capacity.max(1));
        let control_free = crossbeam_queue::ArrayQueue::new(control_capacity.max(1));
        let terminal_free = crossbeam_queue::ArrayQueue::new(terminal_capacity.max(1));
        debug_assert!(capacity + control_capacity + terminal_capacity < (1 << FRAME_INDEX_BITS));
        let cells = (0..capacity + control_capacity + terminal_capacity)
            .map(|index| {
                if index < capacity {
                    let _ = free.push(index);
                } else if index < capacity + control_capacity {
                    let _ = control_free.push(index);
                } else {
                    let _ = terminal_free.push(index);
                }
                FrameCell {
                    generation: AtomicU64::new(0),
                    control_a: AtomicU64::new(0),
                    b_c: AtomicU64::new(0),
                    range: AtomicU64::new(0),
                    next: AtomicU64::new(0),
                }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let owner = NEXT_FRAME_STORE_ID
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                value.checked_add(1)
            })
            .unwrap_or(0);
        Self {
            owner,
            data_capacity: capacity,
            closed: std::sync::atomic::AtomicBool::new(false),
            cells,
            free,
            control_free,
            terminal_free,
            #[cfg(test)]
            decoded_headers: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            transferred_records: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            byte_observations: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    #[cfg(test)]
    fn insert(&self, frame: StoredFrame) -> crate::Result<FrameToken> {
        self.insert_with_priority(frame, false)
    }

    fn insert_with_priority(
        &self,
        frame: StoredFrame,
        priority: bool,
    ) -> crate::Result<FrameToken> {
        if self.owner == 0 || self.closed.load(Ordering::Acquire) {
            return Err(crate::GossipError::ConnectionDropped);
        }
        if frame.frame.body_len > ((1 << 27) - 1)
            || frame.bytes == 0
            || frame
                .offset
                .checked_add(frame.bytes)
                .is_none_or(|end| end > 4 + frame.frame.body_len)
        {
            return Err(invalid("admitted frame range is invalid").into());
        }
        let terminal = matches!(
            frame.frame.kind,
            WireKind::Response
                | WireKind::DirectResponse
                | WireKind::StreamResponseStart
                | WireKind::StreamResponseData
                | WireKind::StreamAbort
        );
        let control = matches!(frame.frame.kind, WireKind::Gossip | WireKind::RouteBind);
        let index = if priority && terminal {
            self.terminal_free.pop()
        } else if priority && control {
            self.control_free.pop()
        } else {
            self.free.pop().or_else(|| {
                // Control progress retains its original reserved inventory
                // after ordinary metadata fills, without expanding any bound.
                control.then(|| self.control_free.pop()).flatten()
            })
        }
        .ok_or(crate::GossipError::WriteQueueFull)?;
        let cell = &self.cells[index];
        let old = cell.generation.load(Ordering::SeqCst);
        let generation = old
            .checked_add(1)
            .filter(|value| *value < (u64::MAX >> FRAME_INDEX_BITS))
            .ok_or(crate::GossipError::WriteQueueFull)?;
        // Atomic fields keep teardown rollback safe even if a consumer has
        // already started copying metadata. A second generation check rejects
        // any recycled snapshot; stale receipts can never mutate successors.
        let control = ((frame.frame.kind as u64) << 27) | frame.frame.body_len as u64;
        cell.control_a
            .store(control | (u64::from(frame.frame.a) << 32), Ordering::SeqCst);
        cell.b_c.store(
            u64::from(frame.frame.b) | (u64::from(frame.frame.c) << 32),
            Ordering::SeqCst,
        );
        cell.range.store(
            frame.offset as u64 | ((frame.bytes as u64) << 32),
            Ordering::SeqCst,
        );
        cell.next
            .store(frame.next.map_or(0, FrameToken::encoded), Ordering::SeqCst);
        cell.generation.store(generation, Ordering::SeqCst);
        let token = FrameToken { index, generation };
        if self.closed.load(Ordering::Acquire) {
            self.release(token);
            return Err(crate::GossipError::ConnectionDropped);
        }
        Ok(token)
    }

    fn get(&self, token: FrameToken) -> io::Result<StoredFrame> {
        let cell = self
            .cells
            .get(token.index)
            .ok_or_else(|| invalid("frame receipt index outside slab"))?;
        if cell.generation.load(Ordering::SeqCst) != token.generation {
            return Err(invalid("stale admitted frame receipt"));
        }
        let control_a = cell.control_a.load(Ordering::SeqCst);
        let b_c = cell.b_c.load(Ordering::SeqCst);
        let range = cell.range.load(Ordering::SeqCst);
        let next = FrameToken::decode(cell.next.load(Ordering::SeqCst));
        if cell.generation.load(Ordering::SeqCst) != token.generation {
            return Err(invalid("recycled admitted frame receipt"));
        }
        Ok(StoredFrame {
            frame: Frame {
                kind: WireKind::from_u8((control_a as u32 >> 27) as u8)
                    .ok_or_else(|| invalid("invalid stored frame kind"))?,
                body_len: (control_a as u32 & ((1 << 27) - 1)) as usize,
                a: (control_a >> 32) as u32,
                b: b_c as u32,
                c: (b_c >> 32) as u32,
            },
            offset: range as u32 as usize,
            bytes: (range >> 32) as usize,
            next,
        })
    }

    fn append(&self, target: &mut AdmittedFrames, source: AdmittedFrames) -> io::Result<()> {
        if !source.is_empty() && source.owner != self.owner {
            return Err(invalid("frame receipt belongs to another physical stream"));
        }
        if !target.is_empty() && target.owner != self.owner {
            self.release_chain(source);
            return Err(invalid("frame chain belongs to another physical stream"));
        }
        if !target.is_empty()
            && !source.is_empty()
            && (target.first == source.first || target.last == source.last)
        {
            // The target still owns these nodes. This defensive alias check
            // must not retire its live chain on behalf of an invalid source.
            return Err(invalid("frame chain cannot append itself"));
        }
        if target.is_empty() && !source.is_empty() {
            target.owner = self.owner;
        }
        if let Some(tail) = target.last {
            if source.first.is_some() {
                let Some(cell) = self.cells.get(tail.index) else {
                    self.release_chain(source);
                    return Err(invalid("frame receipt index outside slab"));
                };
                // A terminal node is linked once. Claim its exact generation
                // before mutation; rollback requests retirement after the
                // final store, including any newly attached suffix.
                if cell
                    .generation
                    .compare_exchange(
                        tail.generation,
                        tail.generation | FRAME_LINKING,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_err()
                {
                    self.release_chain(source);
                    return Err(invalid("stale frame-chain tail"));
                }
                let already_linked = cell.next.load(Ordering::SeqCst) != 0;
                if !already_linked {
                    cell.next.store(
                        source.first.map_or(0, FrameToken::encoded),
                        Ordering::SeqCst,
                    );
                }
                let retired = self.finish_link(tail);
                if already_linked {
                    self.release_chain(source);
                    return Err(invalid("frame-chain tail was already linked"));
                }
                if retired {
                    return Err(invalid("frame chain retired during publication"));
                }
            }
        } else {
            target.first = source.first;
        }
        if source.last.is_some() {
            target.last = source.last;
        }
        Ok(())
    }

    fn return_index(&self, index: usize) {
        let free = if index < self.data_capacity {
            &self.free
        } else if index < self.data_capacity + CONTROL_FRAME_RECEIPT_CAP {
            &self.control_free
        } else {
            &self.terminal_free
        };
        let _ = free.push(index);
    }

    fn finish_retire(&self, token: FrameToken) -> Option<FrameToken> {
        let cell = &self.cells[token.index];
        // Retirement owns the generation before reading the final link. No
        // linker or successor can mutate next until this index is returned.
        let next = FrameToken::decode(cell.next.load(Ordering::SeqCst));
        cell.generation
            .store(token.generation + 1, Ordering::SeqCst);
        self.return_index(token.index);
        next
    }

    fn finish_link(&self, token: FrameToken) -> bool {
        let cell = &self.cells[token.index];
        if cell
            .generation
            .compare_exchange(
                token.generation | FRAME_LINKING,
                token.generation,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
            && cell
                .generation
                .compare_exchange(
                    token.generation | FRAME_LINKING | FRAME_RELEASE_REQUESTED,
                    token.generation | FRAME_RELEASE_REQUESTED,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
        {
            let first = self.finish_retire(token);
            self.release_chain(AdmittedFrames {
                owner: self.owner,
                first,
                last: None,
            });
            return true;
        }
        false
    }

    fn retire(&self, token: FrameToken) -> Option<FrameToken> {
        let cell = self.cells.get(token.index)?;
        match cell.generation.compare_exchange(
            token.generation,
            token.generation | FRAME_RELEASE_REQUESTED,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => self.finish_retire(token),
            Err(state) if state == (token.generation | FRAME_LINKING) => {
                if let Err(state) = cell.generation.compare_exchange(
                    token.generation | FRAME_LINKING,
                    token.generation | FRAME_LINKING | FRAME_RELEASE_REQUESTED,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    // If the single link just completed, claim retirement
                    // before reading its final next. Otherwise the competing
                    // retire owner is responsible for the suffix.
                    if state == token.generation
                        && cell
                            .generation
                            .compare_exchange(
                                token.generation,
                                token.generation | FRAME_RELEASE_REQUESTED,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            )
                            .is_ok()
                    {
                        return self.finish_retire(token);
                    }
                }
                None
            }
            Err(_) => None,
        }
    }

    fn release(&self, token: FrameToken) {
        // The normal consumer already owns the copied next token. It returns
        // only this metadata slot; whole-chain rollback uses retire's result.
        let _ = self.retire(token);
    }

    fn rollback(&self, marker: FrameRollback) {
        self.release_chain(AdmittedFrames {
            owner: marker.owner,
            first: marker.first,
            last: None,
        });
    }

    fn release_chain(&self, mut chain: AdmittedFrames) {
        if !chain.is_empty() && chain.owner != self.owner {
            return;
        }
        while let Some(token) = chain.first {
            chain.first = self.retire(token);
        }
    }

    fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        // Physical teardown is the only slab sweep. It also releases nodes
        // retained by dropped queued commands or pending batches after their
        // producer committed. Old handles can never reopen this slab.
        for (index, cell) in self.cells.iter().enumerate() {
            let generation =
                cell.generation.load(Ordering::SeqCst) & !(FRAME_LINKING | FRAME_RELEASE_REQUESTED);
            if generation & 1 != 0 {
                self.release(FrameToken { index, generation });
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct TerminalWindow {
    // Zero is free; full correlation ID plus a transmitted bit fences stale
    // replies and unsent admission rollback. No per-request allocation.
    slots: [AtomicU64; OUTCOME_CAP],
    available: [tokio::sync::Semaphore; OUTCOME_CAP],
    frames: FrameStore,
}

impl TerminalWindow {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            slots: std::array::from_fn(|_| AtomicU64::new(0)),
            available: std::array::from_fn(|_| tokio::sync::Semaphore::new(1)),
            frames: FrameStore::new(FRAME_RECEIPT_CAP),
        })
    }

    fn receiver() -> Arc<Self> {
        Arc::new(Self {
            slots: std::array::from_fn(|_| AtomicU64::new(0)),
            available: std::array::from_fn(|_| tokio::sync::Semaphore::new(1)),
            frames: FrameStore::new(0),
        })
    }

    #[cfg(test)]
    pub(super) fn metadata_stats(&self) -> (usize, usize, usize, usize) {
        (
            self.frames.decoded_headers.load(Ordering::Relaxed),
            self.frames.transferred_records.load(Ordering::Relaxed),
            self.frames.byte_observations.load(Ordering::Relaxed),
            self.frames.cells.len()
                - self.frames.free.len()
                - self.frames.control_free.len()
                - self.frames.terminal_free.len(),
        )
    }

    #[cfg(test)]
    pub(super) fn occupied(&self) -> usize {
        self.slots
            .iter()
            .filter(|slot| slot.load(Ordering::Acquire) != 0)
            .count()
    }

    fn reserve(&self, id: u32) -> crate::Result<()> {
        if id == 0 {
            return Err(crate::GossipError::WriteQueueFull);
        }
        let index = id as usize & (OUTCOME_CAP - 1);
        let permit = self.available[index].try_acquire().map_err(|_| {
            if self.available[index].is_closed() {
                crate::GossipError::ConnectionDropped
            } else {
                crate::GossipError::WriteQueueFull
            }
        })?;
        self.slots[index]
            .compare_exchange(0, u64::from(id) << 1, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| crate::GossipError::WriteQueueFull)?;
        permit.forget();
        Ok(())
    }

    async fn reserve_wait(&self, id: u32) -> crate::Result<()> {
        if id == 0 {
            return Err(crate::GossipError::WriteQueueFull);
        }
        let index = id as usize & (OUTCOME_CAP - 1);
        let permit = self.available[index]
            .acquire()
            .await
            .map_err(|_| crate::GossipError::ConnectionDropped)?;
        self.slots[index]
            .compare_exchange(0, u64::from(id) << 1, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| crate::GossipError::WriteQueueFull)?;
        permit.forget();
        Ok(())
    }

    fn close(&self) {
        self.frames.close();
        for cell in &self.available {
            cell.close();
        }
    }

    fn transmitted(&self, id: u32) -> io::Result<()> {
        if id == 0 {
            return Err(invalid("zero ask outcome ID"));
        }
        self.slots[id as usize & (OUTCOME_CAP - 1)]
            .compare_exchange(
                u64::from(id) << 1,
                (u64::from(id) << 1) | 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|_| ())
            .map_err(|_| invalid("ask lacks transport-owned outcome admission"))
    }

    fn release(&self, id: u32, transmitted: bool) {
        if id == 0 {
            return;
        }
        let expected = (u64::from(id) << 1) | u64::from(transmitted);
        let index = id as usize & (OUTCOME_CAP - 1);
        if self.slots[index]
            .compare_exchange(expected, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.available[index].add_permits(1);
        }
    }

    #[cfg(test)]
    pub(super) fn admit_scatter(&self, chunks: &[bytes::Bytes]) -> crate::Result<Admission<'_>> {
        self.admit_scatter_checked(chunks, None)
    }

    pub(super) fn admit_scatter_checked(
        &self,
        chunks: &[bytes::Bytes],
        max_body: Option<usize>,
    ) -> crate::Result<Admission<'_>> {
        let mut admission = Admission {
            window: self,
            ids: [0; OUTCOME_CAP],
            count: 0,
            reserved: 0,
            cells: 0,
            committed: false,
            frames: AdmittedFrames::default(),
            record_metadata: false,
            priority: false,
            published: None,
            max_body: None,
        };
        admission.record_metadata = true;
        admission.max_body = max_body;
        let mut cursor = ScatterCursor {
            chunks,
            index: 0,
            offset: 0,
        };
        while cursor.has_data() {
            let mut header = [0; 16];
            cursor.copy_header(&mut header[..4])?;
            let control = control(&header)?;
            let extra = Frame {
                kind: control.kind,
                body_len: control.body_len,
                a: 0,
                b: 0,
                c: 0,
            }
            .metadata_len();
            let body = control
                .body_len
                .checked_sub(extra)
                .ok_or_else(|| invalid("short scatter ask header"))?;
            cursor.copy_header(&mut header[4..4 + extra])?;
            admission.header_control(&header[..4 + extra], control)?;
            cursor.skip(body)?;
        }
        admission.reserve_all()?;
        Ok(admission)
    }

    #[cfg(test)]
    pub(super) fn admit_header(&self, bytes: &[u8]) -> crate::Result<Admission<'_>> {
        let mut admission = Admission {
            window: self,
            ids: [0; OUTCOME_CAP],
            count: 0,
            reserved: 0,
            cells: 0,
            committed: false,
            frames: AdmittedFrames::default(),
            record_metadata: false,
            priority: false,
            published: None,
            max_body: None,
        };
        admission.header(bytes)?;
        admission.reserve_all()?;
        Ok(admission)
    }

    #[cfg(test)]
    pub(super) fn admit_frames(&self, bytes: &[u8]) -> crate::Result<Admission<'_>> {
        self.admit_frames_checked(bytes, None)
    }

    pub(super) fn admit_frames_checked(
        &self,
        bytes: &[u8],
        max_body: Option<usize>,
    ) -> crate::Result<Admission<'_>> {
        let mut admission = Admission {
            window: self,
            ids: [0; OUTCOME_CAP],
            count: 0,
            reserved: 0,
            cells: 0,
            committed: false,
            frames: AdmittedFrames::default(),
            record_metadata: false,
            priority: false,
            published: None,
            max_body: None,
        };
        admission.record_metadata = true;
        admission.max_body = max_body;
        admission.frames(bytes)?;
        admission.reserve_all()?;
        Ok(admission)
    }

    #[cfg(test)]
    pub(super) fn admit_wait<'a>(
        &'a self,
        payload: &super::WritePayload,
    ) -> impl std::future::Future<Output = crate::Result<Admission<'a>>> + 'a {
        let collected = self.collect(payload, None);
        async move {
            let mut admission = collected?;
            admission.reserve_all_wait().await?;
            Ok(admission)
        }
    }

    pub(super) fn admit_checked(
        &self,
        payload: &super::WritePayload,
        max_body: usize,
    ) -> crate::Result<Admission<'_>> {
        let mut admission = self.collect(payload, Some(max_body))?;
        admission.reserve_all()?;
        Ok(admission)
    }

    pub(super) fn admit_immediate_checked(
        &self,
        payload: &super::WritePayload,
        max_body: usize,
    ) -> crate::Result<Admission<'_>> {
        let mut admission = self.collect_with_priority(payload, Some(max_body), true)?;
        admission.reserve_all()?;
        Ok(admission)
    }

    pub(super) fn admit_wait_checked<'a>(
        &'a self,
        payload: &super::WritePayload,
        max_body: usize,
    ) -> impl std::future::Future<Output = crate::Result<Admission<'a>>> + 'a {
        self.admit_wait_checked_with_priority(payload, max_body, false)
    }

    pub(super) fn admit_immediate_wait_checked<'a>(
        &'a self,
        payload: &super::WritePayload,
        max_body: usize,
    ) -> impl std::future::Future<Output = crate::Result<Admission<'a>>> + 'a {
        self.admit_wait_checked_with_priority(payload, max_body, true)
    }

    fn admit_wait_checked_with_priority<'a>(
        &'a self,
        payload: &super::WritePayload,
        max_body: usize,
        priority: bool,
    ) -> impl std::future::Future<Output = crate::Result<Admission<'a>>> + 'a {
        // Finite receipt saturation is typed admission backpressure, just as
        // for the original multi-frame API. Once admitted, only the genuine
        // wire-outcome window/queue wait remains; no second metadata waiter,
        // wakeup, timer or duplicate bookkeeping is added to the healthy path.
        let collected = self.collect_with_priority(payload, Some(max_body), priority);
        async move {
            let mut admission = collected?;
            admission.reserve_all_wait().await?;
            Ok(admission)
        }
    }

    pub(super) async fn admit_header_wait(&self, header: &[u8]) -> crate::Result<Admission<'_>> {
        let mut admission = Admission {
            window: self,
            ids: [0; OUTCOME_CAP],
            count: 0,
            reserved: 0,
            cells: 0,
            committed: false,
            frames: AdmittedFrames::default(),
            record_metadata: true,
            priority: false,
            published: None,
            max_body: None,
        };
        admission.header(header)?;
        admission.reserve_all_wait().await?;
        Ok(admission)
    }

    fn collect(
        &self,
        payload: &super::WritePayload,
        max_body: Option<usize>,
    ) -> crate::Result<Admission<'_>> {
        self.collect_with_priority(payload, max_body, false)
    }

    fn collect_with_priority(
        &self,
        payload: &super::WritePayload,
        max_body: Option<usize>,
        priority: bool,
    ) -> crate::Result<Admission<'_>> {
        use super::WritePayload;
        let mut admission = Admission {
            window: self,
            ids: [0; OUTCOME_CAP],
            count: 0,
            reserved: 0,
            cells: 0,
            committed: false,
            frames: AdmittedFrames::default(),
            record_metadata: true,
            priority,
            published: None,
            max_body: None,
        };
        if !matches!(payload, WritePayload::TrustedFrame(_)) {
            admission.max_body = max_body;
        }
        match payload {
            WritePayload::Single(bytes) | WritePayload::TrustedFrame(bytes) => {
                admission.record_metadata = true;
                admission.frames(bytes)?;
            }
            WritePayload::HeaderPayload { header, .. }
            | WritePayload::HeaderPooled { header, .. } => admission.header(header)?,
            WritePayload::HeaderInline {
                header, header_len, ..
            }
            | WritePayload::HeaderInlineAligned {
                header, header_len, ..
            }
            | WritePayload::HeaderInlinePooled {
                header, header_len, ..
            } => admission.header(&header[..usize::from(*header_len)])?,
            WritePayload::HeaderInline32 { header, .. } => admission.header(header)?,
            WritePayload::DirectAskInline { header, .. } => admission.header(header)?,
            WritePayload::Buf { buf, expected_len } => {
                if buf.remaining() != *expected_len {
                    return Err(invalid("buffer differs from its declared frame length").into());
                }
                let control = control(buf.chunk())?;
                if 4 + control.body_len != *expected_len {
                    return Err(invalid("buffer is not exactly its declared frame").into());
                }
                admission.record_metadata = true;
                admission.header_control(buf.chunk(), control)?;
            }
        }
        Ok(admission)
    }
}

/// Cold generic scatter validation gathers only split fixed metadata; it
/// never compacts or clones an application payload owner.
struct ScatterCursor<'a, B> {
    chunks: &'a [B],
    index: usize,
    offset: usize,
}
impl<B: AsRef<[u8]>> ScatterCursor<'_, B> {
    fn has_data(&mut self) -> bool {
        while self.index < self.chunks.len()
            && self.offset == self.chunks[self.index].as_ref().len()
        {
            self.index += 1;
            self.offset = 0;
        }
        self.index < self.chunks.len()
    }
    fn copy_header(&mut self, mut target: &mut [u8]) -> io::Result<()> {
        while !target.is_empty() {
            if !self.has_data() {
                return Err(invalid("truncated scatter header"));
            }
            let source = &self.chunks[self.index].as_ref()[self.offset..];
            let count = source.len().min(target.len());
            target[..count].copy_from_slice(&source[..count]);
            self.offset += count;
            target = &mut target[count..];
        }
        Ok(())
    }
    fn skip(&mut self, mut count: usize) -> io::Result<()> {
        while count != 0 {
            if !self.has_data() {
                return Err(invalid("truncated scatter body"));
            }
            let take = count.min(self.chunks[self.index].as_ref().len() - self.offset);
            self.offset += take;
            count -= take;
        }
        Ok(())
    }
}

pub(super) struct Admission<'a> {
    window: &'a TerminalWindow,
    ids: [u32; OUTCOME_CAP],
    count: usize,
    reserved: usize,
    cells: u64,
    committed: bool,
    frames: AdmittedFrames,
    record_metadata: bool,
    priority: bool,
    published: Option<FrameRollback>,
    max_body: Option<usize>,
}
impl Admission<'_> {
    fn frames(&mut self, mut remaining: &[u8]) -> crate::Result<()> {
        while !remaining.is_empty() {
            let control = control(remaining)?;
            let length = 4 + control.body_len;
            if length > remaining.len() {
                return Err(invalid("incomplete admitted frame").into());
            }
            self.header_control(remaining, control)?;
            remaining = &remaining[length..];
        }
        Ok(())
    }
    fn header(&mut self, bytes: &[u8]) -> crate::Result<()> {
        self.header_control(bytes, control(bytes)?)
    }

    fn header_control(&mut self, bytes: &[u8], control: framing::Control) -> crate::Result<()> {
        if let Some(max) = self.max_body
            && control.body_len > max
        {
            return Err(crate::GossipError::MessageTooLarge {
                size: control.body_len,
                max,
            });
        }
        let id = if self.record_metadata {
            let frame =
                InitialOutcome::from_metadata(control.kind, control.body_len, &bytes[4..])?.0;
            #[cfg(test)]
            self.window
                .frames
                .decoded_headers
                .fetch_add(1, Ordering::Relaxed);
            let token = self.window.frames.insert_with_priority(
                StoredFrame {
                    frame,
                    offset: 0,
                    bytes: 4 + frame.body_len,
                    next: None,
                },
                self.priority,
            )?;
            self.window.frames.append(
                &mut self.frames,
                AdmittedFrames {
                    owner: self.window.frames.owner,
                    first: Some(token),
                    last: Some(token),
                },
            )?;
            match frame.kind {
                WireKind::Ask
                | WireKind::ActorAsk
                | WireKind::RoutedActorAsk
                | WireKind::DirectAsk => frame.a,
                WireKind::StreamStart => frame.b,
                _ => 0,
            }
        } else {
            match control.kind {
                WireKind::Ask
                | WireKind::ActorAsk
                | WireKind::RoutedActorAsk
                | WireKind::DirectAsk => word(bytes, 4)?,
                WireKind::StreamStart => word(bytes, 8)?,
                _ => 0,
            }
        };
        if id != 0 {
            if self.count == OUTCOME_CAP {
                return Err(crate::GossipError::WriteQueueFull);
            }
            // The wire window cannot represent two IDs in the same cell in
            // one batch. Reject before waiting or publishing any bytes.
            let cell = 1u64 << (id & 63);
            if self.cells & cell != 0 {
                return Err(crate::GossipError::WriteQueueFull);
            }
            self.cells |= cell;
            self.ids[self.count] = id;
            self.count += 1;
        }
        Ok(())
    }
    fn order_cells(&mut self) {
        // A fixed <=64-item metadata batch, no allocation. Global cell order
        // prevents overlapping async batches from holding a circular wait.
        if self.count > 1 {
            self.ids[..self.count].sort_unstable_by_key(|id| id & 63);
        }
    }
    fn reserve_all(&mut self) -> crate::Result<()> {
        self.order_cells();
        for id in &self.ids[..self.count] {
            self.window.reserve(*id)?;
            self.reserved += 1;
        }
        Ok(())
    }
    async fn reserve_all_wait(&mut self) -> crate::Result<()> {
        self.order_cells();
        for id in &self.ids[..self.count] {
            self.window.reserve_wait(*id).await?;
            self.reserved += 1;
        }
        Ok(())
    }
    pub(super) fn fragments(&self) -> FrameFragments<'_> {
        FrameFragments {
            store: &self.window.frames,
            cursor: self.frames.first,
            offset: 0,
        }
    }

    pub(super) fn frames_receipt(&mut self) -> AdmittedFrames {
        let frames = std::mem::take(&mut self.frames);
        if !frames.is_empty() {
            self.published = Some(FrameRollback {
                owner: frames.owner,
                first: frames.first,
            });
        }
        frames
    }

    pub(super) fn commit(mut self) {
        self.committed = true;
    }
}
impl Drop for Admission<'_> {
    fn drop(&mut self) {
        self.window
            .frames
            .release_chain(std::mem::take(&mut self.frames));
        if !self.committed {
            if let Some(marker) = self.published.take() {
                self.window.frames.rollback(marker);
            }
            for id in &self.ids[..self.reserved] {
                self.window.release(*id, false);
            }
        }
    }
}

pub(super) struct FrameFragments<'a> {
    store: &'a FrameStore,
    cursor: Option<FrameToken>,
    offset: usize,
}

pub(super) struct FrameTransfer<'a> {
    store: &'a FrameStore,
    frames: AdmittedFrames,
    published: Option<FrameRollback>,
    committed: bool,
}

impl FrameFragments<'_> {
    pub(super) fn next(&mut self, mut length: usize) -> crate::Result<FrameTransfer<'_>> {
        let mut transfer = FrameTransfer {
            store: self.store,
            frames: AdmittedFrames::default(),
            published: None,
            committed: false,
        };
        while length != 0 {
            let token = self
                .cursor
                .ok_or_else(|| invalid("fragment exceeds admitted bytes"))?;
            let entry = self.store.get(token)?;
            let take = length.min(entry.bytes - self.offset);
            let priority = token.index >= self.store.data_capacity;
            let fragment = self.store.insert_with_priority(
                StoredFrame {
                    frame: entry.frame,
                    offset: entry.offset + self.offset,
                    bytes: take,
                    next: None,
                },
                priority,
            )?;
            self.store.append(
                &mut transfer.frames,
                AdmittedFrames {
                    owner: self.store.owner,
                    first: Some(fragment),
                    last: Some(fragment),
                },
            )?;
            self.offset += take;
            length -= take;
            if self.offset == entry.bytes {
                self.offset = 0;
                self.cursor = entry.next;
            }
        }
        Ok(transfer)
    }
}

impl FrameTransfer<'_> {
    pub(super) fn frames_receipt(&mut self) -> AdmittedFrames {
        let frames = std::mem::take(&mut self.frames);
        if !frames.is_empty() {
            self.published = Some(FrameRollback {
                owner: frames.owner,
                first: frames.first,
            });
        }
        frames
    }

    pub(super) fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for FrameTransfer<'_> {
    fn drop(&mut self) {
        self.store.release_chain(std::mem::take(&mut self.frames));
        if !self.committed
            && let Some(marker) = self.published.take()
        {
            self.store.rollback(marker);
        }
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn word(bytes: &[u8], offset: usize) -> io::Result<u32> {
    let data = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| invalid("truncated outcome header"))?;
    Ok(u32::from_be_bytes(
        data.try_into().expect("four-byte field"),
    ))
}
fn control(bytes: &[u8]) -> io::Result<framing::Control> {
    framing::decode_control(word(bytes, 0)?.to_be_bytes())
        .ok_or_else(|| invalid("invalid outcome control"))
}

#[derive(Clone, Copy, Default)]
struct ResponseStream {
    stream: u32,
    correlation: u32,
    remaining: usize,
    next_chunk: u32,
}

// Preallocated sparse radix index: at most 32 direct child steps for any
// stream ID, independent of occupancy. No table sweep, heap insertion, hash
// collision fallback, or incidental refcount operation on chunk completion.
const RADIX_NODES: usize = OUTCOME_CAP * 32 + 1;
const ABSENT: u16 = u16::MAX;
#[derive(Clone, Copy)]
struct RadixNode {
    child: [u16; 2],
    slot: u16,
}
struct StreamReceipts {
    nodes: [RadixNode; RADIX_NODES],
    free_node: u16,
    entries: [ResponseStream; OUTCOME_CAP],
    free_slots: [u16; OUTCOME_CAP],
    free_len: usize,
}
impl StreamReceipts {
    fn new() -> Self {
        let mut nodes = std::array::from_fn(|index| RadixNode {
            child: [
                if index + 1 < RADIX_NODES {
                    (index + 1) as u16
                } else {
                    ABSENT
                },
                ABSENT,
            ],
            slot: ABSENT,
        });
        nodes[0].child = [ABSENT; 2];
        Self {
            nodes,
            free_node: 1,
            entries: [ResponseStream::default(); OUTCOME_CAP],
            free_slots: std::array::from_fn(|index| index as u16),
            free_len: OUTCOME_CAP,
        }
    }
    fn leaf(&self, key: u32) -> Option<usize> {
        let mut node = 0;
        for shift in (0..32).rev() {
            let child = self.nodes[node].child[((key >> shift) & 1) as usize];
            if child == ABSENT {
                return None;
            }
            node = usize::from(child);
        }
        Some(node)
    }
    fn get_mut(&mut self, key: u32) -> Option<&mut ResponseStream> {
        let leaf = self.leaf(key)?;
        let slot = self.nodes[leaf].slot;
        if slot == ABSENT {
            None
        } else {
            Some(&mut self.entries[usize::from(slot)])
        }
    }
    fn insert(&mut self, entry: ResponseStream) -> io::Result<()> {
        if entry.stream == 0 {
            return Err(invalid("zero stream ID"));
        }
        if self.free_len == 0 {
            return Err(invalid("stream outcome capacity exhausted"));
        }
        let mut node = 0;
        for shift in (0..32).rev() {
            let bit = ((entry.stream >> shift) & 1) as usize;
            let mut child = self.nodes[node].child[bit];
            if child == ABSENT {
                child = self.free_node;
                if child == ABSENT {
                    return Err(invalid("stream radix capacity exhausted"));
                }
                self.free_node = self.nodes[usize::from(child)].child[0];
                self.nodes[usize::from(child)] = RadixNode {
                    child: [ABSENT; 2],
                    slot: ABSENT,
                };
                self.nodes[node].child[bit] = child;
            }
            node = usize::from(child);
        }
        if self.nodes[node].slot != ABSENT {
            return Err(invalid("duplicate stream outcome"));
        }
        self.free_len -= 1;
        let slot = self.free_slots[self.free_len];
        self.nodes[node].slot = slot;
        self.entries[usize::from(slot)] = entry;
        Ok(())
    }
    fn remove(&mut self, key: u32) -> Option<ResponseStream> {
        let mut path = [0u16; 33];
        let mut node = 0;
        for depth in 0..32 {
            let bit = ((key >> (31 - depth)) & 1) as usize;
            let child = self.nodes[node].child[bit];
            if child == ABSENT {
                return None;
            }
            path[depth + 1] = child;
            node = usize::from(child);
        }
        let slot = self.nodes[node].slot;
        if slot == ABSENT {
            return None;
        }
        let entry = self.entries[usize::from(slot)];
        self.nodes[node].slot = ABSENT;
        self.free_slots[self.free_len] = slot;
        self.free_len += 1;
        for depth in (1..=32).rev() {
            let index = usize::from(path[depth]);
            if self.nodes[index].child != [ABSENT; 2] || self.nodes[index].slot != ABSENT {
                break;
            }
            self.nodes[index].child[0] = self.free_node;
            self.free_node = path[depth];
            let bit = ((key >> (32 - depth)) & 1) as usize;
            self.nodes[usize::from(path[depth - 1])].child[bit] = ABSENT;
        }
        Some(entry)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Frame {
    kind: WireKind,
    body_len: usize,
    a: u32,
    b: u32,
    c: u32,
}
impl Frame {
    fn matches_header(self, header: &[u8]) -> bool {
        let Ok(control) = framing::try_encode_control(self.kind, self.body_len) else {
            return false;
        };
        if header.get(..4) != Some(control.as_slice()) {
            return false;
        }
        [self.a, self.b, self.c]
            .into_iter()
            .take(self.metadata_len() / 4)
            .enumerate()
            .all(|(index, value)| word(header, 4 + index * 4).ok() == Some(value))
    }

    fn metadata_len(self) -> usize {
        match self.kind {
            WireKind::Ask
            | WireKind::ActorAsk
            | WireKind::RoutedActorAsk
            | WireKind::DirectAsk
            | WireKind::Response
            | WireKind::DirectResponse
            | WireKind::StreamAbort => 4,
            WireKind::StreamStart | WireKind::StreamResponseStart => 12,
            WireKind::StreamData | WireKind::StreamResponseData => 8,
            _ => 0,
        }
    }
    fn metadata(&mut self, bytes: &[u8]) -> io::Result<()> {
        if bytes.len() >= 4 {
            self.a = word(bytes, 0)?;
        }
        if bytes.len() >= 8 {
            self.b = word(bytes, 4)?;
        }
        if bytes.len() >= 12 {
            self.c = word(bytes, 8)?;
        }
        Ok(())
    }
}

/// First-frame ownership crosses the accept-path/IO-task handoff together
/// with reassembly. This is immutable metadata, consumed once at construction.
#[derive(Clone, Copy, Debug)]
pub(crate) struct InitialOutcome(Frame);

impl InitialOutcome {
    pub(super) fn from_metadata(
        kind: WireKind,
        body_len: usize,
        metadata: &[u8],
    ) -> io::Result<Self> {
        let mut frame = Frame {
            kind,
            body_len,
            a: 0,
            b: 0,
            c: 0,
        };
        let len = frame.metadata_len();
        if body_len < len || metadata.len() < len {
            return Err(invalid("truncated parsed outcome metadata"));
        }
        frame.metadata(&metadata[..len])?;
        Ok(Self(frame))
    }
}

/// Read ownership moves into the existing parser once, at IO-task startup.
/// Only complete parser frames consume credits, including discarded bodies.
/// No byte observer runs alongside the parser on production connections.
pub(super) struct ReadOutcomes {
    observer: Box<Observer>,
    sent: Arc<TerminalWindow>,
    received: Arc<TerminalWindow>,
}

impl ReadOutcomes {
    pub(super) fn complete(&mut self, frame: InitialOutcome) -> io::Result<()> {
        self.observer
            .finish(frame.0, &self.sent, &self.received, false)
    }
}
impl InitialOutcome {
    pub(crate) fn from_read(result: &crate::handle::MessageReadResult) -> Option<Self> {
        use crate::handle::MessageReadResult;
        let (kind, body_len, a, b, c) = match result {
            MessageReadResult::AskRaw { correlation_id, .. }
            | MessageReadResult::DirectAsk { correlation_id, .. }
            | MessageReadResult::Gossip(_, Some(correlation_id)) => {
                (WireKind::Ask, 12, *correlation_id, 0, 0)
            }
            MessageReadResult::Actor {
                msg_type,
                correlation_id,
                ..
            } if *msg_type == crate::MessageType::ActorAsk as u8 => {
                (WireKind::ActorAsk, 28, *correlation_id, 0, 0)
            }
            MessageReadResult::Response { correlation_id, .. }
            | MessageReadResult::DirectResponse { correlation_id, .. }
            | MessageReadResult::AskNack { correlation_id, .. } => {
                (WireKind::Response, 12, *correlation_id, 0, 0)
            }
            MessageReadResult::Streaming {
                msg_type,
                correlation_id,
                stream_header,
                chunk_data,
                ..
            } if *msg_type == crate::MessageType::StreamStart as u8
                || *msg_type == crate::MessageType::StreamResponseStart as u8 =>
            {
                let response = *msg_type == crate::MessageType::StreamResponseStart as u8;
                let kind = if response {
                    WireKind::StreamResponseStart
                } else {
                    WireKind::StreamStart
                };
                let overhead = if response {
                    framing::STREAM_RESPONSE_START_HEADER_LEN
                } else {
                    framing::STREAM_REQUEST_START_HEADER_LEN
                };
                (
                    kind,
                    overhead + chunk_data.len(),
                    stream_header.stream_id as u32,
                    *correlation_id,
                    stream_header.total_size as u32,
                )
            }
            _ => return None,
        };
        Some(Self(Frame {
            kind,
            body_len,
            a,
            b,
            c,
        }))
    }
}

/// The common case decodes borrowed fixed metadata directly. Scratch is used
/// only for fragmented headers. Tell/gossip/pubsub application bodies are
/// skipped in O(1), without an extra prefix or application-payload copy.
struct Observer {
    #[cfg(test)]
    byte_observations: usize,
    scratch: [u8; 12],
    captured: usize,
    current: Option<Frame>,
    remaining: usize,
    metadata_ready: bool,
    streams: StreamReceipts,
    requests: StreamReceipts,
}
impl Observer {
    fn new() -> Self {
        Self {
            #[cfg(test)]
            byte_observations: 0,
            scratch: [0; 12],
            captured: 0,
            current: None,
            remaining: 0,
            metadata_ready: false,
            streams: StreamReceipts::new(),
            requests: StreamReceipts::new(),
        }
    }
    fn consume(
        &mut self,
        mut bytes: &[u8],
        sent: &TerminalWindow,
        received: &TerminalWindow,
        outbound: bool,
    ) -> io::Result<()> {
        #[cfg(test)]
        if !bytes.is_empty() {
            self.byte_observations += 1;
        }
        while !bytes.is_empty() {
            if self.current.is_none() {
                let control = if self.captured == 0 && bytes.len() >= 4 {
                    let result = control(bytes)?;
                    bytes = &bytes[4..];
                    result
                } else {
                    let count = (4 - self.captured).min(bytes.len());
                    self.scratch[self.captured..self.captured + count]
                        .copy_from_slice(&bytes[..count]);
                    self.captured += count;
                    bytes = &bytes[count..];
                    if self.captured < 4 {
                        break;
                    }
                    self.captured = 0;
                    control(&self.scratch[..4])?
                };
                let frame = Frame {
                    kind: control.kind,
                    body_len: control.body_len,
                    a: 0,
                    b: 0,
                    c: 0,
                };
                if frame.body_len < frame.metadata_len() {
                    return Err(invalid("truncated outcome metadata"));
                }
                self.remaining = frame.body_len;
                self.metadata_ready = frame.metadata_len() == 0;
                self.current = Some(frame);
            }
            let mut frame = self.current.expect("frame control established");
            if !self.metadata_ready {
                let needed = frame.metadata_len();
                if self.captured == 0 && bytes.len() >= needed {
                    frame.metadata(&bytes[..needed])?;
                    bytes = &bytes[needed..];
                    self.remaining -= needed;
                } else {
                    let count = (needed - self.captured).min(bytes.len());
                    self.scratch[self.captured..self.captured + count]
                        .copy_from_slice(&bytes[..count]);
                    self.captured += count;
                    bytes = &bytes[count..];
                    self.remaining -= count;
                    if self.captured < needed {
                        break;
                    }
                    frame.metadata(&self.scratch[..needed])?;
                    self.captured = 0;
                }
                self.metadata_ready = true;
                self.current = Some(frame);
            }
            let count = self.remaining.min(bytes.len());
            self.remaining -= count;
            bytes = &bytes[count..];
            if self.remaining == 0 {
                self.finish(frame, sent, received, outbound)?;
                self.current = None;
            }
        }
        Ok(())
    }
    fn finish(
        &mut self,
        frame: Frame,
        sent: &TerminalWindow,
        received: &TerminalWindow,
        outbound: bool,
    ) -> io::Result<()> {
        let request = match frame.kind {
            WireKind::Ask | WireKind::ActorAsk | WireKind::RoutedActorAsk | WireKind::DirectAsk => {
                frame.a
            }
            WireKind::StreamStart => frame.b,
            _ => 0,
        };
        if request != 0 {
            if outbound {
                sent.transmitted(request)?;
            } else {
                received
                    .reserve(request)
                    .map_err(|_| invalid("peer exceeded negotiated terminal outcome window"))?;
            }
        }
        let terminal = match frame.kind {
            WireKind::StreamStart | WireKind::StreamResponseStart => {
                if frame.a == 0 {
                    return Err(invalid("zero stream ID"));
                }
                let response = frame.kind == WireKind::StreamResponseStart;
                let overhead = if response {
                    framing::STREAM_RESPONSE_START_HEADER_LEN
                } else {
                    framing::STREAM_REQUEST_START_HEADER_LEN
                };
                let payload = frame
                    .body_len
                    .checked_sub(overhead)
                    .ok_or_else(|| invalid("truncated stream start"))?;
                let remaining = (frame.c as usize)
                    .checked_sub(payload)
                    .ok_or_else(|| invalid("stream length overflow"))?;
                if response && remaining == 0 {
                    Some(frame.b)
                } else {
                    if frame.b != 0 && remaining != 0 {
                        let entry = ResponseStream {
                            stream: frame.a,
                            correlation: frame.b,
                            remaining,
                            next_chunk: 1,
                        };
                        if response {
                            self.streams.insert(entry)?;
                        } else {
                            self.requests.insert(entry)?;
                        }
                    }
                    None
                }
            }
            WireKind::StreamData | WireKind::StreamResponseData => {
                if frame.a == 0 {
                    return Err(invalid("zero stream ID"));
                }
                let payload = frame
                    .body_len
                    .checked_sub(8)
                    .ok_or_else(|| invalid("truncated stream data"))?;
                let response = frame.kind == WireKind::StreamResponseData;
                let table = if response {
                    &mut self.streams
                } else {
                    &mut self.requests
                };
                let completed = if let Some(entry) = table.get_mut(frame.a) {
                    if frame.b != entry.next_chunk {
                        return Err(invalid("out-of-order outcome chunk"));
                    }
                    entry.next_chunk = entry
                        .next_chunk
                        .checked_add(1)
                        .ok_or_else(|| invalid("chunk namespace exhausted"))?;
                    entry.remaining = entry
                        .remaining
                        .checked_sub(payload)
                        .ok_or_else(|| invalid("stream length overflow"))?;
                    entry.remaining == 0
                } else {
                    false
                };
                if completed {
                    let entry = table.remove(frame.a).expect("completed entry exists");
                    if response {
                        Some(entry.correlation)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            WireKind::Response | WireKind::DirectResponse => Some(frame.a),
            WireKind::StreamAbort => {
                if frame.a == 0 {
                    return Err(invalid("zero stream ID"));
                }
                // A leased response cancellation follows Abort with its
                // terminal response; Abort alone cannot free that credit.
                self.streams.remove(frame.a);
                if let Some(entry) = self.requests.remove(frame.a) {
                    // Fully written request cancellation is ordered before
                    // every successor on this physical byte stream.
                    if outbound {
                        sent.release(entry.correlation, true);
                    } else {
                        received.release(entry.correlation, false);
                    }
                }
                None
            }
            _ => None,
        };
        if let Some(id) = terminal {
            if outbound {
                received.release(id, false);
            } else {
                sent.release(id, true);
            }
        }
        Ok(())
    }
}

/// IO-owned metadata for at most 64 complete frames. This is inline in the
/// existing batch/pending owner, never allocated per queue entry. Larger
/// batches refill only after the previous window has committed. The byte
/// cursor is monotonic; no payload owner is copied or reference-counted.
/// Metadata lives until completion/drop of its exact pending command.
pub(super) struct WriteFramePlan {
    frames: [Option<StoredFrame>; OUTCOME_CAP],
    len: usize,
    start: usize,
    end: usize,
    revision: u64,
    owner: u64,
    admitted: AdmittedFrames,
    admitted_only: bool,
    #[cfg(test)]
    prepared_receipts: usize,
    #[cfg(test)]
    fallback_decodes: usize,
}

impl WriteFramePlan {
    pub(super) fn new() -> Self {
        Self {
            frames: [None; OUTCOME_CAP],
            len: 0,
            start: 0,
            end: 0,
            revision: 0,
            owner: 0,
            admitted: AdmittedFrames::default(),
            admitted_only: false,
            #[cfg(test)]
            prepared_receipts: 0,
            #[cfg(test)]
            fallback_decodes: 0,
        }
    }

    pub(super) fn record_header(&mut self, header: &[u8]) -> io::Result<()> {
        if self.len == OUTCOME_CAP {
            return Ok(());
        }
        #[cfg(test)]
        {
            self.fallback_decodes += 1;
        }
        let control = control(header)?;
        let frame = InitialOutcome::from_metadata(control.kind, control.body_len, &header[4..])?;
        self.push(StoredFrame {
            frame: frame.0,
            offset: 0,
            bytes: 4 + frame.0.body_len,
            next: None,
        });
        Ok(())
    }

    fn push(&mut self, frame: StoredFrame) {
        self.end += frame.bytes;
        let previous = self
            .len
            .checked_sub(1)
            .and_then(|index| self.frames[index].as_mut());
        if let Some(previous) = previous
            && previous.frame == frame.frame
            && previous.offset + previous.bytes == frame.offset
        {
            previous.bytes += frame.bytes;
        } else {
            self.frames[self.len] = Some(frame);
            self.len += 1;
        }
        self.revision = 0;
        #[cfg(test)]
        {
            self.prepared_receipts += 1;
        }
    }

    pub(super) fn has_admitted(&self) -> bool {
        self.admitted_only
    }

    pub(super) fn is_full(&self, pending_headers: usize) -> bool {
        self.len.saturating_add(pending_headers) >= OUTCOME_CAP || !self.admitted.is_empty()
    }

    fn append_cached(&mut self, source: &mut Self) -> io::Result<()> {
        if source.len == 0 && source.admitted.is_empty() {
            return Ok(());
        }
        if self.start != 0
            || source.start != 0
            || self.revision != 0
            || source.revision != 0
            || self.len + source.len > OUTCOME_CAP
            || !self.admitted.is_empty()
            || !source.admitted.is_empty()
            || (self.owner != 0 && source.owner != 0 && self.owner != source.owner)
        {
            return Err(invalid("cannot append incompatible frame plans"));
        }
        if self.owner == 0 {
            self.owner = source.owner;
        }
        self.admitted_only |= source.admitted_only;
        for frame in source.frames[..source.len].iter().flatten() {
            self.push(*frame);
        }
        *source = Self::new();
        Ok(())
    }

    fn collect_admitted(&mut self, store: &FrameStore) -> io::Result<()> {
        while self.len < OUTCOME_CAP {
            let Some(token) = self.admitted.first else {
                break;
            };
            let entry = store.get(token)?;
            self.admitted.first = entry.next;
            if entry.next.is_none() {
                self.admitted.last = None;
            }
            self.push(entry);
            #[cfg(test)]
            store.transferred_records.fetch_add(1, Ordering::Relaxed);
            store.release(token);
        }
        Ok(())
    }

    fn fill<B: AsRef<[u8]>>(
        &mut self,
        store: &FrameStore,
        chunks: &[B],
        offset: usize,
    ) -> io::Result<()> {
        if offset == self.end {
            self.len = 0;
            self.start = offset;
            self.revision = 0;
        }
        if offset < self.start || offset > self.end {
            return Err(invalid("writer advanced outside its frame plan"));
        }
        self.collect_admitted(store)?;
        if self.len == OUTCOME_CAP || self.admitted_only {
            return Ok(());
        }
        let mut cursor = ScatterCursor {
            chunks,
            index: 0,
            offset: 0,
        };
        cursor.skip(self.end)?;
        while self.len < OUTCOME_CAP && cursor.has_data() {
            let mut header = [0; 16];
            cursor.copy_header(&mut header[..4])?;
            #[cfg(test)]
            {
                self.fallback_decodes += 1;
            }
            let control = control(&header)?;
            let mut frame = Frame {
                kind: control.kind,
                body_len: control.body_len,
                a: 0,
                b: 0,
                c: 0,
            };
            let metadata_len = frame.metadata_len();
            let body = frame
                .body_len
                .checked_sub(metadata_len)
                .ok_or_else(|| invalid("truncated writer frame metadata"))?;
            cursor.copy_header(&mut header[4..4 + metadata_len])?;
            frame.metadata(&header[4..4 + metadata_len])?;
            cursor.skip(body)?;
            self.push(StoredFrame {
                frame,
                offset: 0,
                bytes: 4 + frame.body_len,
                next: None,
            });
        }
        Ok(())
    }
}

/// A shared owned-chunks command retains only indices and byte progress.
/// Its immutable records remain in the fixed slab until their bytes commit;
/// the sole IO owner's existing 64-receipt array is the only decoded cache.
#[derive(Default)]
pub(super) struct AdmittedCursor {
    base: usize,
    end: usize,
    revision: u64,
    owner: u64,
}

/// Metadata supplied by the IO owner's existing frame writer. Opaque generic
/// buffers explicitly select the compatibility decoder instead.
pub(super) trait OutcomeWrite: AsyncWrite {
    fn prepare_frames(&mut self, _frames: &[(&[u8], usize)]) -> io::Result<()> {
        Ok(())
    }

    fn append_admitted(
        &mut self,
        _plan: &mut WriteFramePlan,
        _frames: AdmittedFrames,
    ) -> io::Result<()> {
        Ok(())
    }

    fn append_header(&mut self, plan: &mut WriteFramePlan, header: &[u8]) -> io::Result<()> {
        plan.record_header(header)
    }

    fn append_plan(
        &mut self,
        target: &mut WriteFramePlan,
        source: &mut WriteFramePlan,
    ) -> io::Result<()> {
        target.append_cached(source)
    }

    fn begin_admitted(&mut self, _frames: AdmittedFrames) -> io::Result<()> {
        Ok(())
    }

    fn prepare_owned_chunks(
        &mut self,
        _cursor: &mut AdmittedCursor,
        _frames: &mut AdmittedFrames,
        _offset: usize,
    ) -> io::Result<usize> {
        Ok(usize::MAX)
    }

    fn advance_admitted(
        &mut self,
        _cursor: &mut AdmittedCursor,
        _frames: &mut AdmittedFrames,
        _offset: usize,
    ) -> io::Result<()> {
        Ok(())
    }

    fn prepare_batch<B: AsRef<[u8]>>(
        &mut self,
        _plan: &mut WriteFramePlan,
        _chunks: &[B],
        _offset: usize,
    ) -> io::Result<usize> {
        Ok(usize::MAX)
    }

    fn prepare_buf(&mut self, _header: &[u8], _remaining: usize) -> io::Result<()> {
        Ok(())
    }

    fn prepare_opaque(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn frame_plan_revision(&self) -> u64 {
        0
    }
}

#[cfg(test)]
impl OutcomeWrite for tokio::io::DuplexStream {}
#[cfg(test)]
impl OutcomeWrite for tokio::io::Sink {}

#[derive(Clone, Copy)]
struct WriteReceipt {
    frame: Frame,
    remaining: usize,
}

/// Owned exclusively by one IO task. Rust's exclusive poll receiver prevents
/// a local poll_read from observing a terminal while poll_write has delivered
/// bytes but not yet committed its transmitted bit. No other task has either
/// Observer; producer handles can only reserve unsent sender cells.
pub(crate) struct OutcomeStream<S> {
    inner: S,
    sent: Arc<TerminalWindow>,
    received: Arc<TerminalWindow>,
    read: Option<Box<Observer>>,
    write: Observer,
    prepared: [Option<WriteReceipt>; 64],
    prepared_len: usize,
    prepared_index: usize,
    prepared_revision: u64,
    prepared_offset: usize,
    invalid_initial: bool,
    pending_admission: AdmittedFrames,
    admitted_buf: bool,
}
impl<S> OutcomeStream<S> {
    pub(crate) fn new(inner: S, sent: Arc<TerminalWindow>) -> Self {
        Self {
            inner,
            sent,
            received: TerminalWindow::receiver(),
            read: Some(Box::new(Observer::new())),
            write: Observer::new(),
            prepared: [None; 64],
            prepared_len: 0,
            prepared_index: 0,
            prepared_revision: 0,
            prepared_offset: 0,
            invalid_initial: false,
            pending_admission: AdmittedFrames::default(),
            admitted_buf: false,
        }
    }
    #[cfg(test)]
    pub(super) fn opaque_write_observations(&self) -> usize {
        self.write.byte_observations
    }

    pub(super) fn take_read_outcomes(&mut self) -> ReadOutcomes {
        ReadOutcomes {
            observer: self.read.take().expect("read ownership moves once"),
            sent: Arc::clone(&self.sent),
            received: Arc::clone(&self.received),
        }
    }

    fn account_write(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.prepared_index == self.prepared_len {
            if self.prepared_len != 0 && !bytes.is_empty() {
                return Err(invalid("write exceeded prepared frame receipts"));
            }
            #[cfg(test)]
            if !bytes.is_empty() {
                self.sent
                    .frames
                    .byte_observations
                    .fetch_add(1, Ordering::Relaxed);
            }
            return self.write.consume(bytes, &self.sent, &self.received, true);
        }
        let mut remaining = bytes.len();
        while remaining != 0 {
            let Some(receipt) = self.prepared[self.prepared_index].as_mut() else {
                return Err(invalid("missing writer frame receipt"));
            };
            let count = remaining.min(receipt.remaining);
            remaining -= count;
            receipt.remaining -= count;
            if receipt.remaining == 0 {
                self.write
                    .finish(receipt.frame, &self.sent, &self.received, true)?;
                self.prepared_index += 1;
                if self.prepared_index == self.prepared_len && remaining != 0 {
                    return Err(invalid("write exceeded prepared frame receipts"));
                }
            }
        }
        self.prepared_offset = self
            .prepared_offset
            .checked_add(bytes.len())
            .ok_or_else(|| invalid("writer byte offset overflow"))?;
        Ok(())
    }

    pub(crate) fn seed_initial(&mut self, receipt: Option<InitialOutcome>) {
        if let Some(receipt) = receipt {
            self.invalid_initial = self
                .read
                .as_mut()
                .expect("initial receipt precedes parser handoff")
                .finish(receipt.0, &self.sent, &self.received, false)
                .is_err();
        }
    }
}
impl<S: AsyncWrite + Unpin> OutcomeWrite for OutcomeStream<S> {
    fn append_admitted(
        &mut self,
        plan: &mut WriteFramePlan,
        frames: AdmittedFrames,
    ) -> io::Result<()> {
        if plan.owner != 0 && plan.owner != self.sent.frames.owner {
            return Err(invalid("frame plan belongs to another physical stream"));
        }
        let nonempty = !frames.is_empty();
        self.sent.frames.append(&mut plan.admitted, frames)?;
        plan.admitted_only |= nonempty;
        plan.owner = self.sent.frames.owner;
        plan.collect_admitted(&self.sent.frames)
    }

    fn append_header(&mut self, plan: &mut WriteFramePlan, header: &[u8]) -> io::Result<()> {
        let Some(token) = self.pending_admission.first else {
            return plan.record_header(header);
        };
        if plan.len == OUTCOME_CAP
            || !plan.admitted.is_empty()
            || (plan.owner != 0 && plan.owner != self.sent.frames.owner)
        {
            return Err(invalid("typed header exceeded its frame plan"));
        }
        let entry = self.sent.frames.get(token)?;
        if !entry.frame.matches_header(header) {
            return Err(invalid("typed header differs from its producer receipt"));
        }
        plan.owner = self.sent.frames.owner;
        plan.admitted_only = true;
        plan.push(entry);
        self.pending_admission.first = entry.next;
        if entry.next.is_none() {
            self.pending_admission.last = None;
        }
        #[cfg(test)]
        self.sent
            .frames
            .transferred_records
            .fetch_add(1, Ordering::Relaxed);
        self.sent.frames.release(token);
        Ok(())
    }

    fn append_plan(
        &mut self,
        target: &mut WriteFramePlan,
        source: &mut WriteFramePlan,
    ) -> io::Result<()> {
        if (target.owner != 0 && target.owner != self.sent.frames.owner)
            || (source.owner != 0 && source.owner != self.sent.frames.owner)
        {
            return Err(invalid("frame plan belongs to another physical stream"));
        }
        target.append_cached(source)
    }

    fn begin_admitted(&mut self, frames: AdmittedFrames) -> io::Result<()> {
        self.sent
            .frames
            .append(&mut self.pending_admission, frames)?;
        self.admitted_buf = false;
        Ok(())
    }

    fn advance_admitted(
        &mut self,
        cursor: &mut AdmittedCursor,
        frames: &mut AdmittedFrames,
        offset: usize,
    ) -> io::Result<()> {
        if (cursor.owner != 0 && cursor.owner != self.sent.frames.owner)
            || (!frames.is_empty() && frames.owner != self.sent.frames.owner)
            || offset < cursor.base
        {
            return Err(invalid("owned-chunks receipt belongs to another writer"));
        }
        cursor.owner = self.sent.frames.owner;
        while let Some(token) = frames.first {
            let frame = self.sent.frames.get(token)?;
            if offset < cursor.base + frame.bytes {
                break;
            }
            cursor.base += frame.bytes;
            frames.first = frame.next;
            if frames.first.is_none() {
                frames.last = None;
            }
            #[cfg(test)]
            self.sent
                .frames
                .transferred_records
                .fetch_add(1, Ordering::Relaxed);
            self.sent.frames.release(token);
        }
        Ok(())
    }

    fn prepare_owned_chunks(
        &mut self,
        cursor: &mut AdmittedCursor,
        frames: &mut AdmittedFrames,
        offset: usize,
    ) -> io::Result<usize> {
        self.admitted_buf = false;
        self.advance_admitted(cursor, frames, offset)?;
        if cursor.revision != 0 && cursor.revision == self.prepared_revision && offset < cursor.end
        {
            if offset != self.prepared_offset {
                return Err(invalid("owned chunks changed the committed byte cursor"));
            }
            return Ok(cursor.end - offset);
        }
        if self.write.current.is_some() || self.write.captured != 0 {
            return Err(invalid("owned chunks interrupted an opaque frame"));
        }
        let active = if self.prepared_index < self.prepared_len {
            self.prepared[self.prepared_index]
                .filter(|receipt| receipt.remaining < 4 + receipt.frame.body_len)
        } else {
            None
        };
        let mut token = frames.first;
        let mut count = 0;
        let mut skip = offset - cursor.base;
        let mut end = cursor.base;
        while let Some(current) = token {
            if count == OUTCOME_CAP {
                break;
            }
            let entry = self.sent.frames.get(current)?;
            if entry.offset != 0 || entry.bytes != 4 + entry.frame.body_len || skip >= entry.bytes {
                return Err(invalid("owned chunks changed admitted frame boundaries"));
            }
            let receipt = WriteReceipt {
                frame: entry.frame,
                remaining: entry.bytes - skip,
            };
            if count == 0
                && active.is_some_and(|active| {
                    active.frame != receipt.frame || active.remaining != receipt.remaining
                })
            {
                return Err(invalid(
                    "owned chunks interrupted a partially committed frame",
                ));
            }
            self.prepared[count] = Some(receipt);
            count += 1;
            end += entry.bytes;
            skip = 0;
            token = entry.next;
        }
        self.prepared_revision = self
            .prepared_revision
            .checked_add(1)
            .ok_or_else(|| invalid("writer receipt revision exhausted"))?;
        self.prepared_len = count;
        self.prepared_index = 0;
        cursor.revision = self.prepared_revision;
        cursor.end = end;
        self.prepared_offset = offset;
        Ok(end - offset)
    }

    fn prepare_batch<B: AsRef<[u8]>>(
        &mut self,
        plan: &mut WriteFramePlan,
        chunks: &[B],
        offset: usize,
    ) -> io::Result<usize> {
        if plan.owner != 0 && plan.owner != self.sent.frames.owner {
            return Err(invalid("frame plan belongs to another physical stream"));
        }
        self.admitted_buf = false;
        plan.owner = self.sent.frames.owner;
        if plan.revision != 0 && plan.revision == self.prepared_revision && offset < plan.end {
            if offset != self.prepared_offset {
                return Err(invalid("frame plan changed the committed byte cursor"));
            }
            return Ok(plan.end - offset);
        }
        plan.fill(&self.sent.frames, chunks, offset)?;
        if plan.len == 0 {
            return Ok(0);
        }
        if self.write.current.is_some() || self.write.captured != 0 {
            return Err(invalid("frame plan interrupted an opaque frame"));
        }
        let active = if self.prepared_index < self.prepared_len {
            self.prepared[self.prepared_index]
                .filter(|receipt| receipt.remaining < 4 + receipt.frame.body_len)
        } else {
            None
        };
        let mut skip = offset - plan.start;
        let mut count = 0;
        for frame in plan.frames[..plan.len].iter().flatten() {
            let total = frame.bytes;
            if skip >= total {
                skip -= total;
                continue;
            }
            let receipt = WriteReceipt {
                frame: frame.frame,
                remaining: 4 + frame.frame.body_len - frame.offset - skip,
            };
            skip = 0;
            if count == 0
                && active.is_some_and(|active| {
                    active.frame != receipt.frame || active.remaining != receipt.remaining
                })
            {
                return Err(invalid("writer changed a partially committed frame"));
            }
            self.prepared[count] = Some(receipt);
            count += 1;
        }
        self.prepared_revision = self
            .prepared_revision
            .checked_add(1)
            .ok_or_else(|| invalid("writer receipt revision exhausted"))?;
        self.prepared_index = 0;
        self.prepared_len = count;
        plan.revision = self.prepared_revision;
        self.prepared_offset = offset;
        Ok(plan.end - offset)
    }

    fn prepare_buf(&mut self, header: &[u8], remaining: usize) -> io::Result<()> {
        if let Some(token) = self.pending_admission.first {
            let entry = self.sent.frames.get(token)?;
            if entry.next.is_some()
                || entry.bytes != remaining
                || entry.offset != 0
                || !entry.frame.matches_header(header)
            {
                return Err(invalid("buffer does not match its admitted frame"));
            }
            if self.write.current.is_some()
                || self.write.captured != 0
                || (self.prepared_index < self.prepared_len
                    && self.prepared[self.prepared_index].is_some_and(|active| {
                        active.remaining < 4 + active.frame.body_len
                            && (active.frame != entry.frame || active.remaining != remaining)
                    }))
            {
                return Err(invalid("buffer interrupted a partially committed frame"));
            }
            self.prepared_revision = self
                .prepared_revision
                .checked_add(1)
                .ok_or_else(|| invalid("writer receipt revision exhausted"))?;
            self.prepared[0] = Some(WriteReceipt {
                frame: entry.frame,
                remaining,
            });
            self.prepared_index = 0;
            self.prepared_len = 1;
            self.prepared_offset = 0;
            self.pending_admission = AdmittedFrames::default();
            #[cfg(test)]
            self.sent
                .frames
                .transferred_records
                .fetch_add(1, Ordering::Relaxed);
            self.sent.frames.release(token);
            self.admitted_buf = true;
            return Ok(());
        }
        if self.admitted_buf && self.prepared_index < self.prepared_len {
            if self.prepared[self.prepared_index]
                .is_some_and(|receipt| receipt.remaining == remaining)
            {
                return Ok(());
            }
            return Err(invalid("buffer changed its admitted remaining length"));
        }
        if let Some(active) = self.prepared.get(self.prepared_index).copied().flatten()
            && self.prepared_index < self.prepared_len
            && active.remaining < 4 + active.frame.body_len
        {
            if active.remaining != remaining {
                return Err(invalid("buffer changed a partially committed frame"));
            }
            return Ok(());
        }
        if remaining == 0 {
            return Ok(());
        }
        self.prepare_frames(&[(header, 0)])
    }

    fn prepare_frames(&mut self, frames: &[(&[u8], usize)]) -> io::Result<()> {
        if !self.pending_admission.is_empty() {
            if frames.len() != 1 || frames[0].1 != 0 {
                return Err(invalid("typed admission requires its original frame start"));
            }
            let token = self.pending_admission.first.unwrap();
            let entry = self.sent.frames.get(token)?;
            return self.prepare_buf(frames[0].0, entry.bytes);
        }
        if self.admitted_buf && self.prepared_index < self.prepared_len {
            if frames.len() != 1
                || frames[0].1 != self.prepared_offset
                || !self.prepared[self.prepared_index]
                    .is_some_and(|receipt| receipt.frame.matches_header(frames[0].0))
            {
                return Err(invalid("typed frame changed its admitted byte cursor"));
            }
            return Ok(());
        }
        self.admitted_buf = false;
        if frames.is_empty() || frames.len() > self.prepared.len() {
            return Err(invalid("writer receipt count outside fixed budget"));
        }
        if self.write.current.is_some() || self.write.captured != 0 {
            return Err(invalid("typed frame interrupted an opaque frame"));
        }
        let active = if self.prepared_index < self.prepared_len {
            self.prepared[self.prepared_index]
                .filter(|receipt| receipt.remaining < 4 + receipt.frame.body_len)
        } else {
            None
        };
        for (index, (header, offset)) in frames.iter().enumerate() {
            let control = control(header)?;
            let frame =
                InitialOutcome::from_metadata(control.kind, control.body_len, &header[4..])?.0;
            let remaining = (4 + frame.body_len)
                .checked_sub(*offset)
                .filter(|remaining| *remaining != 0)
                .ok_or_else(|| invalid("writer frame offset outside receipt"))?;
            if index == 0
                && active
                    .is_some_and(|active| active.frame != frame || active.remaining != remaining)
            {
                return Err(invalid("writer changed a partially committed frame"));
            }
            self.prepared[index] = Some(WriteReceipt { frame, remaining });
        }
        self.prepared_revision = self
            .prepared_revision
            .checked_add(1)
            .ok_or_else(|| invalid("writer receipt revision exhausted"))?;
        self.prepared_index = 0;
        self.prepared_len = frames.len();
        self.prepared_offset = frames[0].1;
        Ok(())
    }

    fn frame_plan_revision(&self) -> u64 {
        self.prepared_revision
    }

    fn prepare_opaque(&mut self) -> io::Result<()> {
        self.admitted_buf = false;
        if self.prepared_index < self.prepared_len
            && self.prepared[self.prepared_index]
                .is_some_and(|receipt| receipt.remaining < 4 + receipt.frame.body_len)
        {
            return Err(invalid("opaque write interrupted a typed frame"));
        }
        self.prepared_revision = self
            .prepared_revision
            .checked_add(1)
            .ok_or_else(|| invalid("writer receipt revision exhausted"))?;
        self.prepared_index = 0;
        self.prepared_len = 0;
        self.prepared_offset = 0;
        Ok(())
    }
}

impl<S> Drop for OutcomeStream<S> {
    fn drop(&mut self) {
        // Wake borrowed semaphore waiters even when callers retain old handles.
        self.sent.close();
        self.received.close();
    }
}
impl<S: AsyncRead + Unpin> AsyncRead for OutcomeStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.invalid_initial {
            return Poll::Ready(Err(invalid("invalid initial outcome handoff")));
        }
        let before = buf.filled().len();
        match Pin::new(&mut this.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => Poll::Ready(match &mut this.read {
                Some(read) => {
                    read.consume(&buf.filled()[before..], &this.sent, &this.received, false)
                }
                None => Ok(()),
            }),
            other => other,
        }
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for OutcomeStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.invalid_initial {
            return Poll::Ready(Err(invalid("invalid initial outcome handoff")));
        }
        match Pin::new(&mut this.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(count)) => {
                Poll::Ready(this.account_write(&buf[..count]).map(|()| count))
            }
            other => other,
        }
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.invalid_initial {
            return Poll::Ready(Err(invalid("invalid initial outcome handoff")));
        }
        match Pin::new(&mut this.inner).poll_write_vectored(cx, bufs) {
            Poll::Ready(Ok(count)) => {
                let mut remaining = count;
                for buf in bufs {
                    let length = remaining.min(buf.len());
                    if let Err(error) = this.account_write(&buf[..length]) {
                        return Poll::Ready(Err(error));
                    }
                    remaining -= length;
                    if remaining == 0 {
                        break;
                    }
                }
                Poll::Ready(Ok(count))
            }
            other => other,
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn ask(id: u32) -> [u8; 16] {
        framing::write_direct_ask_header(id, u64::from(id), 0)
    }
    fn reply(id: u32) -> [u8; 16] {
        framing::write_ask_nack_header(id, framing::AskNackReason::Backpressure)
    }
    fn occupied(window: &TerminalWindow) -> usize {
        window
            .slots
            .iter()
            .filter(|slot| slot.load(Ordering::Acquire) != 0)
            .count()
    }

    #[tokio::test]
    async fn prepared_batch_accounts_exact_partial_write_boundaries() {
        let sent = TerminalWindow::new();
        let (inner, mut peer) = tokio::io::duplex(20);
        let mut stream = OutcomeStream::new(inner, sent);
        stream.received.reserve(1).unwrap();
        stream.received.reserve(2).unwrap();
        let first = reply(1);
        let second = reply(2);
        stream.prepare_frames(&[(&first, 0), (&second, 0)]).unwrap();
        assert_eq!(occupied(&stream.received), 2, "preparation is not a write");
        let written = stream
            .write_vectored(&[io::IoSlice::new(&first), io::IoSlice::new(&second)])
            .await
            .unwrap();
        assert_eq!(written, 20);
        assert_eq!(occupied(&stream.received), 1);
        assert_eq!(stream.received.slots[2].load(Ordering::Acquire), 4);
        assert!(stream.write.current.is_none());
        assert_eq!(
            stream.write.captured, 0,
            "typed writes bypass the byte decoder"
        );
        assert!(stream.prepare_frames(&[(&reply(3), 4)]).is_err());
        assert!(stream.prepare_opaque().is_err());
        let mut prefix = [0; 20];
        peer.read_exact(&mut prefix).await.unwrap();
        assert_eq!(&prefix[..16], &first);
        assert_eq!(&prefix[16..], &second[..4]);
        stream.prepare_frames(&[(&second, 4)]).unwrap();
        stream.write_all(&second[4..]).await.unwrap();
        assert_eq!(occupied(&stream.received), 0);
        stream.received.reserve(65).unwrap();
        assert_eq!(occupied(&stream.received), 1);
    }

    #[tokio::test]
    async fn prepared_request_cannot_be_completed_before_actual_write() {
        let sent = TerminalWindow::new();
        let (inner, mut peer) = tokio::io::duplex(8);
        let mut stream = OutcomeStream::new(inner, sent.clone());
        let frame = ask(1);
        sent.admit_header(&frame).unwrap().commit();
        stream.prepare_frames(&[(&frame, 0)]).unwrap();
        let mut unsolicited = Observer::new();
        unsolicited
            .consume(&reply(1), &sent, &stream.received, false)
            .unwrap();
        assert_eq!(occupied(&sent), 1);
        assert_eq!(sent.slots[1].load(Ordering::Acquire), 2);
        assert_eq!(
            stream
                .write_vectored(&[io::IoSlice::new(&frame)])
                .await
                .unwrap(),
            8
        );
        assert_eq!(sent.slots[1].load(Ordering::Acquire), 2);
        let mut prefix = [0; 8];
        peer.read_exact(&mut prefix).await.unwrap();
        stream.prepare_frames(&[(&frame, 8)]).unwrap();
        stream.write_all(&frame[8..]).await.unwrap();
        assert_eq!(sent.slots[1].load(Ordering::Acquire), 3);
        unsolicited
            .consume(&reply(1), &sent, &stream.received, false)
            .unwrap();
        assert_eq!(occupied(&sent), 0);
        for _ in 0..8 {
            stream.prepare_frames(&[(&reply(0), 0)]).unwrap();
            // Avoid an IO dependency: this is exactly the successful-byte
            // accounting seam used by poll_write after the socket returns n.
            stream.account_write(&reply(0)).unwrap();
        }
        assert_eq!(stream.received.available[0].available_permits(), 1);
    }

    #[test]
    fn repeated_zero_terminals_do_not_inflate_free_cell() {
        let sent = TerminalWindow::new();
        let received = TerminalWindow::new();
        let mut read = Observer::new();
        let mut write = Observer::new();
        for _ in 0..128 {
            read.consume(&reply(0), &sent, &received, false).unwrap();
            write.consume(&reply(0), &sent, &received, true).unwrap();
        }
        assert!(sent.transmitted(0).is_err());
        assert_eq!(sent.slots[0].load(Ordering::Acquire), 0);
        assert_eq!(sent.available[0].available_permits(), 1);
        assert_eq!(received.available[0].available_permits(), 1);
        sent.reserve(64).unwrap();
        assert!(sent.reserve(128).is_err());
        sent.release(64, false);
        assert_eq!(sent.available[0].available_permits(), 1);
    }

    #[test]
    fn malformed_peer_cannot_exceed_receiver_outcome_credit() {
        let sent = TerminalWindow::new();
        let received = TerminalWindow::new();
        let mut reader = Observer::new();
        for id in 1..=OUTCOME_CAP as u32 {
            reader.consume(&ask(id), &sent, &received, false).unwrap();
        }
        assert_eq!(occupied(&received), OUTCOME_CAP);
        let error = reader
            .consume(&ask(65), &sent, &received, false)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            occupied(&received),
            OUTCOME_CAP,
            "over-credit input must not evict any accepted terminal owner"
        );
        assert_eq!(received.slots[1].load(Ordering::Acquire), 2);
    }

    #[test]
    fn cancelled_waiter_does_not_recycle_transport_outcome_authority() {
        let sent = TerminalWindow::new();
        let received = TerminalWindow::new();
        let mut write = Observer::new();
        let mut read = Observer::new();
        for id in 1..=OUTCOME_CAP as u32 {
            sent.admit_frames(&ask(id)).unwrap().commit();
            write.consume(&ask(id), &sent, &received, true).unwrap();
        }
        // There are deliberately no user waiter owners. Dropping/cancelling
        // all of them cannot make a 65th terminal record fit on the peer.
        assert_eq!(occupied(&sent), OUTCOME_CAP);
        assert!(sent.admit_frames(&ask(65)).is_err());
        read.consume(&reply(1)[..15], &sent, &received, false)
            .unwrap();
        assert!(
            sent.admit_frames(&ask(65)).is_err(),
            "partial terminal is not a receipt"
        );
        read.consume(&reply(1)[15..], &sent, &received, false)
            .unwrap();
        sent.admit_frames(&ask(65)).unwrap().commit();
        write.consume(&ask(65), &sent, &received, true).unwrap();
        read.consume(&reply(1), &sent, &received, false).unwrap();
        assert_eq!(
            occupied(&sent),
            OUTCOME_CAP,
            "stale predecessor must preserve successor"
        );
    }

    #[test]
    fn foreign_stream_and_unsent_replies_cannot_return_credit() {
        let sent = TerminalWindow::new();
        let foreign = TerminalWindow::new();
        let received = TerminalWindow::new();
        sent.admit_frames(&ask(7)).unwrap().commit();
        Observer::new()
            .consume(&reply(7), &sent, &received, false)
            .unwrap();
        assert_eq!(
            occupied(&sent),
            1,
            "reply cannot authorize an unsent request"
        );
        Observer::new()
            .consume(&ask(7), &sent, &received, true)
            .unwrap();
        Observer::new()
            .consume(&reply(7), &foreign, &received, false)
            .unwrap();
        assert_eq!(
            occupied(&sent),
            1,
            "another physical stream has no cleanup authority"
        );
        Observer::new()
            .consume(&reply(7), &sent, &received, false)
            .unwrap();
        assert_eq!(
            occupied(&sent),
            0,
            "same-stream terminal must really return authority"
        );
    }

    #[test]
    fn unsent_batch_rollback_reclaims_every_cell() {
        let sent = TerminalWindow::new();
        let mut frames = Vec::new();
        for id in 1..=65 {
            frames.extend_from_slice(&ask(id));
        }
        assert!(sent.admit_frames(&frames).is_err());
        assert_eq!(
            occupied(&sent),
            0,
            "failed all-or-none admission returns every unsent credit"
        );
        let admission = sent.admit_frames(&ask(1)).unwrap();
        assert_eq!(occupied(&sent), 1);
        drop(admission);
        assert_eq!(occupied(&sent), 0);
    }

    #[test]
    fn raw_receiver_overrun_and_duplicate_fail_before_dispatch() {
        let sent = TerminalWindow::new();
        let received = TerminalWindow::new();
        let mut read = Observer::new();
        for id in 1..=64 {
            read.consume(&ask(id), &sent, &received, false).unwrap();
        }
        assert_eq!(occupied(&received), 64);
        let error = read.consume(&ask(65), &sent, &received, false).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            occupied(&received),
            64,
            "accepted outcomes are never evicted"
        );
        assert!(
            Observer::new()
                .consume(&ask(1), &sent, &received, false)
                .is_err()
        );
    }

    #[tokio::test]
    async fn full_terminal_lane_keeps_control_and_reverse_replies_readable() {
        let sent = TerminalWindow::new();
        sent.admit_frames(&ask(100)).unwrap().commit();
        let (mut peer, inner) = tokio::io::duplex(128);
        let mut stream = OutcomeStream::new(inner, sent.clone());
        stream
            .write
            .consume(&ask(100), &sent, &stream.received, true)
            .unwrap();
        let mut nacks = super::super::LocalStreamingQueue::new();
        for id in 1..=64 {
            stream
                .read
                .as_mut()
                .expect("legacy stream installs a read observer")
                .consume(&ask(id), &sent, &stream.received, false)
                .unwrap();
            nacks.queue_ask_nack(reply(id)).unwrap();
        }
        assert!(!nacks.has_room_for_ask_nack());
        // Saturate physical output without involving protocol bytes. The
        // opposite direction must continue, even with all 64 NACKs retained.
        stream.inner.write_all(&[0; 128]).await.unwrap();
        let abort = framing::write_stream_abort_header(777, 2);
        peer.write_all(&abort).await.unwrap();
        peer.write_all(&reply(100)).await.unwrap();
        let mut control = [0; framing::STREAM_DATA_FRAME_HEADER_LEN];
        stream.read_exact(&mut control).await.unwrap();
        assert_eq!(control, abort);
        let mut response = [0; 16];
        stream.read_exact(&mut response).await.unwrap();
        assert_eq!(response, reply(100));
        assert_eq!(occupied(&sent), 0);
        assert_eq!(occupied(&stream.received), 64);
        assert_eq!(
            nacks.pending_ask_nack_count(),
            64,
            "control progress does not discard terminal outcomes"
        );
    }

    #[test]
    fn streaming_receipt_returns_credit_only_after_last_chunk() {
        let sent = TerminalWindow::new();
        let received = TerminalWindow::new();
        sent.admit_frames(&ask(9)).unwrap().commit();
        Observer::new()
            .consume(&ask(9), &sent, &received, true)
            .unwrap();
        let mut read = Observer::new();
        let mut first = framing::try_write_stream_response_start_header(91, 9, 2, 1)
            .unwrap()
            .to_vec();
        first.push(3);
        read.consume(&first, &sent, &received, false).unwrap();
        assert_eq!(occupied(&sent), 1);
        let mut last = framing::try_write_stream_data_header(true, 91, 1, 1)
            .unwrap()
            .to_vec();
        last.push(4);
        read.consume(&last, &sent, &received, false).unwrap();
        assert_eq!(occupied(&sent), 0);
    }
    #[test]
    fn malformed_stream_headers_are_errors_not_panics_or_credit_returns() {
        let sent = TerminalWindow::new();
        let received = TerminalWindow::new();
        sent.admit_frames(&ask(1)).unwrap().commit();
        sent.transmitted(1).unwrap();
        for kind in [WireKind::StreamResponseData, WireKind::StreamData] {
            for length in 0..8 {
                let mut bytes = framing::try_encode_control(kind, length).unwrap().to_vec();
                bytes.resize(4 + length, 0);
                assert!(
                    Observer::new()
                        .consume(&bytes, &sent, &received, false)
                        .is_err()
                );
                assert_eq!(occupied(&sent), 1);
            }
        }
        let zero_stream = framing::try_write_stream_response_start_header(0, 1, 1, 0).unwrap();
        assert!(
            Observer::new()
                .consume(&zero_stream, &sent, &received, false)
                .is_err()
        );
        let empty = framing::try_encode_control(WireKind::Gossip, 0).unwrap();
        Observer::new()
            .consume(&empty, &sent, &received, false)
            .unwrap();
        assert_eq!(occupied(&sent), 1);
    }

    #[test]
    fn radix_receipts_recycle_all_nodes_and_preserve_foreign_entries() {
        let mut table = StreamReceipts::new();
        for round in 0..3 {
            for index in 0..64u32 {
                let id = 1 + index.wrapping_mul(0x0400_0001) + round;
                table
                    .insert(ResponseStream {
                        stream: id,
                        correlation: index + 1,
                        remaining: 2,
                        next_chunk: 1,
                    })
                    .unwrap();
            }
            assert_eq!(table.free_len, 0);
            assert!(
                table
                    .insert(ResponseStream {
                        stream: u32::MAX,
                        ..ResponseStream::default()
                    })
                    .is_err()
            );
            assert!(table.remove(u32::MAX).is_none());
            for index in 0..64u32 {
                let id = 1 + index.wrapping_mul(0x0400_0001) + round;
                assert_eq!(table.get_mut(id).unwrap().correlation, index + 1);
                assert_eq!(table.remove(id).unwrap().correlation, index + 1);
                assert!(table.get_mut(id).is_none());
            }
            assert_eq!(table.free_len, 64);
            assert_eq!(table.nodes[0].child, [ABSENT; 2]);
        }
    }

    #[test]
    fn fragmented_scatter_admits_before_publication_and_rejects_truncation() {
        let sent = TerminalWindow::new();
        let frame = ask(13);
        let chunks: Vec<_> = frame
            .iter()
            .map(|byte| bytes::Bytes::from(vec![*byte]))
            .collect();
        let admission = sent.admit_scatter(&chunks).unwrap();
        assert_eq!(occupied(&sent), 1);
        drop(admission);
        assert_eq!(occupied(&sent), 0);
        assert!(sent.admit_scatter(&chunks[..chunks.len() - 1]).is_err());
        assert_eq!(
            occupied(&sent),
            0,
            "malformed scatter must roll back every reserved ID"
        );
    }

    #[test]
    fn first_frame_handoff_is_charged_before_successor_admission() {
        let sent = TerminalWindow::new();
        let (inner, _peer) = tokio::io::duplex(64);
        let mut stream = OutcomeStream::new(inner, sent);
        stream.seed_initial(Some(InitialOutcome(Frame {
            kind: WireKind::StreamStart,
            body_len: framing::STREAM_REQUEST_START_HEADER_LEN + 1,
            a: 91,
            b: 1,
            c: 2,
        })));
        assert!(!stream.invalid_initial);
        assert_eq!(occupied(&stream.received), 1);
        assert_eq!(
            stream
                .read
                .as_mut()
                .unwrap()
                .requests
                .get_mut(91)
                .unwrap()
                .remaining,
            1
        );
        assert!(stream.received.reserve(65).is_err());
        let abort = framing::write_stream_abort_header(91, 1);
        stream
            .read
            .as_mut()
            .unwrap()
            .consume(&abort, &stream.sent, &stream.received, false)
            .unwrap();
        assert_eq!(occupied(&stream.received), 0);
        assert!(stream.read.as_mut().unwrap().requests.get_mut(91).is_none());
        stream.received.reserve(65).unwrap();
    }

    #[test]
    fn incomplete_request_abort_returns_credit_in_wire_order_only() {
        let sent = TerminalWindow::new();
        let received = TerminalWindow::new();
        let header = framing::try_write_stream_request_start_header(91, 1, 2, 2, 3, 1).unwrap();
        sent.admit_header(&header).unwrap().commit();
        let mut first = header.to_vec();
        first.push(1);
        let mut writer = Observer::new();
        let mut reader = Observer::new();
        writer.consume(&first, &sent, &received, true).unwrap();
        reader
            .consume(&first, &TerminalWindow::new(), &received, false)
            .unwrap();
        let abort = framing::write_stream_abort_header(91, 1);
        writer
            .consume(&abort[..abort.len() - 1], &sent, &received, true)
            .unwrap();
        assert!(sent.admit_frames(&ask(65)).is_err());
        writer
            .consume(&abort[abort.len() - 1..], &sent, &received, true)
            .unwrap();
        assert_eq!(occupied(&sent), 0);
        assert_eq!(
            occupied(&received),
            1,
            "sender cleanup cannot mutate receiver authority"
        );
        reader
            .consume(&abort, &TerminalWindow::new(), &received, false)
            .unwrap();
        assert_eq!(occupied(&received), 0);
        sent.admit_frames(&ask(65)).unwrap().commit();
    }

    #[test]
    fn async_admission_future_does_not_require_payload_sync() {
        fn require_send<T: Send>(_: T) {}

        let window = TerminalWindow::new();
        let payload = super::super::WritePayload::Buf {
            buf: Box::new(bytes::Bytes::copy_from_slice(&ask(1))),
            expected_len: 16,
        };
        require_send(window.admit_wait(&payload));
    }

    #[tokio::test]
    async fn async_admission_waits_for_terminal_without_requiring_caller_retry() {
        let sent = TerminalWindow::new();
        sent.admit_frames(&ask(1)).unwrap().commit();
        sent.transmitted(1).unwrap();
        let payload = super::super::WritePayload::Single(bytes::Bytes::copy_from_slice(&ask(65)));
        let mut successor = Box::pin(sent.admit_wait(&payload));
        assert!(futures::poll!(successor.as_mut()).is_pending());
        sent.release(1, false);
        assert!(
            futures::poll!(successor.as_mut()).is_pending(),
            "waiter cancellation cannot return a transmitted credit"
        );
        sent.release(1, true);
        successor.await.unwrap().commit();
        assert_eq!(occupied(&sent), 1);
        assert_eq!(sent.slots[1].load(Ordering::Acquire), 65 << 1);
    }

    #[tokio::test]
    async fn cancelling_waiting_batch_returns_its_unsent_prefix_and_close_wakes_waiters() {
        let sent = TerminalWindow::new();
        sent.admit_frames(&ask(2)).unwrap().commit();
        let mut batch = ask(1).to_vec();
        batch.extend_from_slice(&ask(66));
        let payload = super::super::WritePayload::Single(bytes::Bytes::from(batch));
        let mut waiting = Box::pin(sent.admit_wait(&payload));
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        assert_eq!(
            occupied(&sent),
            2,
            "test must reach partial multi-cell admission"
        );
        drop(waiting);
        assert_eq!(
            occupied(&sent),
            1,
            "cancellation returns unsent prefix ownership"
        );
        let payload = super::super::WritePayload::Single(bytes::Bytes::copy_from_slice(&ask(66)));
        let mut waiting = Box::pin(sent.admit_wait(&payload));
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        sent.close();
        assert!(matches!(
            waiting.await,
            Err(crate::GossipError::ConnectionDropped)
        ));
        assert_eq!(
            occupied(&sent),
            1,
            "closing waiters never mutates an existing outcome"
        );
    }

    #[tokio::test]
    async fn admitted_burst_transfers_once_across_receipt_windows_and_revision_changes() {
        let sent = TerminalWindow::new();
        let mut bytes = ask(1).to_vec();
        for _ in 0..128 {
            bytes.extend_from_slice(&framing::write_actor_tell_header(7, 11, 0));
        }
        bytes.extend_from_slice(&ask(2));
        let bytes = bytes::Bytes::from(bytes);
        let payload = super::super::WritePayload::Single(bytes.clone());
        let mut admission = sent.admit_checked(&payload, 1024).unwrap();
        let frames = admission.frames_receipt();
        admission.commit();
        let mut stream = OutcomeStream::new(tokio::io::sink(), sent.clone());
        let mut plan = WriteFramePlan::new();
        stream.append_admitted(&mut plan, frames).unwrap();
        assert_eq!(sent.slots[1].load(Ordering::Acquire), 2);
        assert_eq!(sent.slots[2].load(Ordering::Acquire), 4);
        assert_eq!(sent.metadata_stats(), (130, 64, 0, 66));
        let chunks = [bytes];
        let mut offset = 0;
        let mut windows = 0;
        while offset < chunks[0].len() {
            let budget = stream.prepare_batch(&mut plan, &chunks, offset).unwrap();
            assert!(budget > 0);
            assert!(plan.len <= OUTCOME_CAP);
            stream
                .write_all(&chunks[0][offset..offset + budget])
                .await
                .unwrap();
            offset += budget;
            windows += 1;
            if windows == 1 {
                assert_eq!(sent.slots[1].load(Ordering::Acquire), 3);
                assert_eq!(sent.slots[2].load(Ordering::Acquire), 4);
                let control = reply(0);
                stream.prepare_frames(&[(&control, 0)]).unwrap();
                stream.write_all(&control).await.unwrap();
                assert_ne!(plan.revision, stream.frame_plan_revision());
            }
        }
        assert_eq!(
            windows, 3,
            "fixture must exceed the fixed receipt window twice"
        );
        assert_eq!(plan.prepared_receipts, 130);
        assert_eq!(
            plan.fallback_decodes, 0,
            "admission metadata must never be decoded again"
        );
        assert_eq!(sent.metadata_stats(), (130, 130, 0, 0));
        assert_eq!(sent.slots[2].load(Ordering::Acquire), 5);
        assert_eq!(
            sent.occupied(),
            2,
            "writing requests is not a terminal response"
        );
    }

    #[tokio::test]
    async fn full_data_metadata_budget_keeps_terminal_control_progress() {
        let sent = TerminalWindow::new();
        let tell = framing::write_actor_tell_header(7, 11, 0);
        let mut held = Vec::with_capacity(FRAME_RECEIPT_CAP);
        for _ in 0..FRAME_RECEIPT_CAP {
            held.push(sent.admit_frames(&tell).unwrap());
        }
        assert_eq!(sent.metadata_stats().3, FRAME_RECEIPT_CAP);
        assert!(matches!(
            sent.admit_frames(&tell),
            Err(crate::GossipError::WriteQueueFull)
        ));
        let gossip = framing::write_gossip_frame_prefix(0);
        let mut held_controls = Vec::with_capacity(CONTROL_FRAME_RECEIPT_CAP);
        for _ in 0..CONTROL_FRAME_RECEIPT_CAP {
            held_controls.push(sent.admit_frames(&gossip).unwrap());
        }
        assert!(matches!(
            sent.admit_frames(&gossip),
            Err(crate::GossipError::WriteQueueFull)
        ));
        let mut stream = OutcomeStream::new(tokio::io::sink(), sent.clone());
        stream.received.reserve(1).unwrap();
        let terminal = bytes::Bytes::copy_from_slice(&reply(1));
        let payload = super::super::WritePayload::Single(terminal.clone());
        let mut admission = sent.admit_immediate_checked(&payload, 1024).unwrap();
        let mut plan = WriteFramePlan::new();
        stream
            .append_admitted(&mut plan, admission.frames_receipt())
            .unwrap();
        admission.commit();
        let chunks = [terminal];
        stream.prepare_batch(&mut plan, &chunks, 0).unwrap();
        assert_eq!(stream.received.occupied(), 1);
        stream.write_all(&chunks[0]).await.unwrap();
        assert_eq!(stream.received.occupied(), 0);
        assert_eq!(
            sent.metadata_stats().3,
            FRAME_RECEIPT_CAP + CONTROL_FRAME_RECEIPT_CAP
        );
        assert_eq!(stream.opaque_write_observations(), 0);
        drop(held_controls);
        drop(held);
        assert_eq!(sent.metadata_stats().3, 0);
        let successor = sent.admit_frames(&tell).unwrap();
        assert_eq!(sent.metadata_stats().3, 1);
        drop(successor);
        assert_eq!(sent.metadata_stats().3, 0);
    }

    #[tokio::test]
    async fn metadata_saturation_is_typed_and_drop_restores_admission_without_waiters() {
        let sent = TerminalWindow::new();
        let header = framing::write_actor_tell_header(3, 5, 0);
        let payload = super::super::WritePayload::Single(bytes::Bytes::copy_from_slice(&header));
        let mut held = Vec::with_capacity(FRAME_RECEIPT_CAP);
        for _ in 0..FRAME_RECEIPT_CAP {
            held.push(sent.admit_checked(&payload, 1024).unwrap());
        }
        let before = sent.frames.decoded_headers.load(Ordering::Relaxed);
        assert!(matches!(
            sent.admit_wait_checked(&payload, 1024).await,
            Err(crate::GossipError::WriteQueueFull)
        ));
        assert_eq!(
            sent.frames.decoded_headers.load(Ordering::Relaxed),
            before + 1
        );
        assert_eq!(sent.metadata_stats().3, FRAME_RECEIPT_CAP);
        drop(held.pop().unwrap());
        let admission = sent.admit_wait_checked(&payload, 1024).await.unwrap();
        assert_eq!(
            sent.frames.decoded_headers.load(Ordering::Relaxed),
            before + 2
        );
        assert_eq!(sent.metadata_stats().3, FRAME_RECEIPT_CAP);
        drop(admission);
        drop(held);
        assert_eq!(sent.metadata_stats().3, 0);
    }

    #[test]
    fn permanently_exhausted_metadata_rejects_without_reissuing_generation() {
        let store = FrameStore::new(1);
        store.cells[0]
            .generation
            .store(u64::MAX >> FRAME_INDEX_BITS, Ordering::SeqCst);
        let control = framing::decode_control(
            framing::write_actor_tell_header(3, 5, 0)[..4]
                .try_into()
                .unwrap(),
        )
        .unwrap();
        let frame = StoredFrame {
            frame: Frame {
                kind: control.kind,
                body_len: control.body_len,
                a: 3,
                b: 5,
                c: 0,
            },
            offset: 0,
            bytes: 4 + control.body_len,
            next: None,
        };
        assert!(matches!(
            store.insert(frame),
            Err(crate::GossipError::WriteQueueFull)
        ));
        assert!(matches!(
            store.insert(frame),
            Err(crate::GossipError::WriteQueueFull)
        ));
        assert_eq!(
            store.cells[0].generation.load(Ordering::SeqCst),
            u64::MAX >> FRAME_INDEX_BITS
        );
        assert!(store.free.is_empty());
    }

    #[test]
    fn admitted_receipts_reject_foreign_physical_stream_and_stale_generation() {
        let first = TerminalWindow::new();
        let second = TerminalWindow::new();
        let frame = ask(1);
        let mut admission = first.admit_frames(&frame).unwrap();
        let mut foreign = OutcomeStream::new(tokio::io::sink(), second.clone());
        assert!(
            foreign
                .append_admitted(&mut WriteFramePlan::new(), admission.frames_receipt())
                .is_err()
        );
        assert_eq!(first.occupied(), 1);
        assert_eq!(second.occupied(), 0);
        drop(admission);
        assert_eq!(first.occupied(), 0);
        assert_eq!(first.metadata_stats().3, 0);

        let store = FrameStore::new(1);
        let entry = StoredFrame {
            frame: InitialOutcome::from_metadata(WireKind::ActorTell, 12, &[])
                .unwrap()
                .0,
            offset: 0,
            bytes: 16,
            next: None,
        };
        let old = store.insert(entry).unwrap();
        store.release(old);
        let successor = store.insert(entry).unwrap();
        assert_eq!(
            old.index, successor.index,
            "fixture must reuse the exact cell"
        );
        assert_ne!(old.generation, successor.generation);
        store.release(old);
        assert!(store.get(old).is_err());
        assert!(store.get(successor).is_ok());
        assert_eq!(
            store.free.len(),
            0,
            "stale release cannot free a live successor"
        );
        store.release(successor);
        assert_eq!(store.free.len(), 1);
        store.cells[0]
            .generation
            .store((u64::MAX >> FRAME_INDEX_BITS) - 1, Ordering::Release);
        assert!(matches!(
            store.insert(entry),
            Err(crate::GossipError::WriteQueueFull)
        ));
        assert_eq!(
            store.free.len(),
            0,
            "exhausted generation is permanently retired"
        );
    }

    #[tokio::test]
    async fn fragment_receipts_do_not_commit_or_return_credit_before_last_wire_byte() {
        let sent = TerminalWindow::new();
        let header = framing::try_write_stream_request_start_header(91, 1, 2, 7, 11, 1).unwrap();
        let mut data = header.to_vec();
        data.push(3);
        let admission = sent.admit_frames(&data).unwrap();
        let mut fragments = admission.fragments();
        let mut stream = OutcomeStream::new(tokio::io::sink(), sent.clone());
        let mut written = 0;
        for chunk in data.chunks(3) {
            let mut transfer = fragments.next(chunk.len()).unwrap();
            let mut plan = WriteFramePlan::new();
            stream
                .append_admitted(&mut plan, transfer.frames_receipt())
                .unwrap();
            transfer.commit();
            let chunks = [bytes::Bytes::copy_from_slice(chunk)];
            let budget = stream.prepare_batch(&mut plan, &chunks, 0).unwrap();
            assert_eq!(budget, chunk.len());
            assert_eq!(sent.slots[1].load(Ordering::Acquire), 2);
            stream.write_all(&chunks[0]).await.unwrap();
            written += chunk.len();
            if written < data.len() {
                assert_eq!(sent.slots[1].load(Ordering::Acquire), 2);
                assert!(stream.prepare_frames(&[(&reply(0), 0)]).is_err());
            }
            assert_eq!(plan.fallback_decodes, 0);
        }
        admission.commit();
        assert_eq!(sent.slots[1].load(Ordering::Acquire), 3);
        let abort = framing::write_stream_abort_header(91, 1);
        let mut admission = sent.admit_frames(&abort).unwrap();
        let mut plan = WriteFramePlan::new();
        stream
            .append_admitted(&mut plan, admission.frames_receipt())
            .unwrap();
        admission.commit();
        let chunks = [bytes::Bytes::copy_from_slice(&abort)];
        stream.prepare_batch(&mut plan, &chunks, 0).unwrap();
        stream.write_all(&abort[..abort.len() - 1]).await.unwrap();
        assert_eq!(sent.occupied(), 1);
        stream.write_all(&abort[abort.len() - 1..]).await.unwrap();
        assert_eq!(sent.occupied(), 0);
        assert_eq!(
            sent.metadata_stats().0,
            2,
            "fragments derive metadata without decoding headers"
        );
        assert_eq!(sent.metadata_stats().2, 0);
        assert_eq!(sent.metadata_stats().3, 0);
        sent.admit_header(&ask(65)).unwrap().commit();
        assert_eq!(sent.occupied(), 1);
    }

    #[test]
    fn frame_link_claim_defers_recycle_and_fences_stale_mutation() {
        let store = FrameStore::new(2);
        let entry = StoredFrame {
            frame: InitialOutcome::from_metadata(WireKind::ActorTell, 12, &[])
                .unwrap()
                .0,
            offset: 0,
            bytes: 16,
            next: None,
        };
        let tail = store.insert(entry).unwrap();
        let next = store.insert(entry).unwrap();
        store.cells[tail.index]
            .generation
            .compare_exchange(
                tail.generation,
                tail.generation | FRAME_LINKING,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .unwrap();
        store.release(tail);
        assert_eq!(
            store.free.len(),
            0,
            "a mutating generation cannot be reused"
        );
        assert_eq!(
            store.cells[tail.index].generation.load(Ordering::SeqCst),
            tail.generation | FRAME_LINKING | FRAME_RELEASE_REQUESTED,
        );
        store.cells[tail.index]
            .next
            .store(next.encoded(), Ordering::SeqCst);
        store.finish_link(tail);
        assert_eq!(
            store.free.len(),
            2,
            "deferred rollback also reclaims the linked suffix"
        );
        let successor = store.insert(entry).unwrap();
        let next = store.insert(entry).unwrap();
        assert_eq!(successor.index, tail.index);
        assert_ne!(successor.generation, tail.generation);
        store.finish_link(tail);
        store.release(tail);
        let mut stale = AdmittedFrames {
            owner: store.owner,
            first: Some(tail),
            last: Some(tail),
        };
        assert!(
            store
                .append(
                    &mut stale,
                    AdmittedFrames {
                        owner: store.owner,
                        first: Some(next),
                        last: Some(next),
                    },
                )
                .is_err()
        );
        assert!(store.get(successor).unwrap().next.is_none());
        assert_eq!(
            store.free.len(),
            1,
            "failed append returns its consumed source"
        );
        assert!(
            store
                .get(FrameToken {
                    index: store.cells.len(),
                    generation: 1,
                })
                .is_err()
        );
        store.release(next);
        store.release(successor);
        assert_eq!(store.free.len(), 2);
    }

    #[test]
    fn whole_chain_rollback_during_link_reclaims_late_suffix() {
        let store = FrameStore::new(3);
        let entry = StoredFrame {
            frame: InitialOutcome::from_metadata(WireKind::ActorTell, 12, &[])
                .unwrap()
                .0,
            offset: 0,
            bytes: 16,
            next: None,
        };
        let first = store.insert(entry).unwrap();
        let tail = store.insert(entry).unwrap();
        let late = store.insert(entry).unwrap();
        let mut chain = AdmittedFrames {
            owner: store.owner,
            first: Some(first),
            last: Some(first),
        };
        store
            .append(
                &mut chain,
                AdmittedFrames {
                    owner: store.owner,
                    first: Some(tail),
                    last: Some(tail),
                },
            )
            .unwrap();
        store.cells[tail.index]
            .generation
            .compare_exchange(
                tail.generation,
                tail.generation | FRAME_LINKING,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .unwrap();
        store.release_chain(chain);
        assert_eq!(store.free.len(), 1);
        assert!(store.get(first).is_err());
        assert_eq!(
            store.cells[tail.index].generation.load(Ordering::SeqCst),
            tail.generation | FRAME_LINKING | FRAME_RELEASE_REQUESTED
        );
        store.cells[tail.index]
            .next
            .store(late.encoded(), Ordering::SeqCst);
        assert!(store.finish_link(tail));
        assert_eq!(
            store.free.len(),
            3,
            "rollback must include a suffix linked after its snapshot"
        );
        assert!(store.get(tail).is_err());
        assert!(store.get(late).is_err());
        let successors = [
            store.insert(entry).unwrap(),
            store.insert(entry).unwrap(),
            store.insert(entry).unwrap(),
        ];
        for old in [first, tail, late] {
            store.release(old);
        }
        assert_eq!(store.free.len(), 0);
        for successor in successors {
            assert!(store.get(successor).is_ok());
            store.release(successor);
        }
        assert_eq!(store.free.len(), 3);
    }

    #[test]
    fn warm_admitted_metadata_has_no_allocation_or_payload_owner_change() {
        use crate::ask_forwarder::allocation_evidence;

        let sent = TerminalWindow::new();
        let mut stream = OutcomeStream::new(tokio::io::sink(), sent.clone());
        let mut bytes = framing::write_actor_tell_header(7, 11, 3).to_vec();
        bytes.extend_from_slice(b"one");
        let payload = super::super::WritePayload::Single(bytes::Bytes::from(bytes));
        let super::super::WritePayload::Single(bytes) = &payload else {
            unreachable!();
        };
        let chunks = std::slice::from_ref(bytes);
        let payload_pointer = bytes.as_ptr();
        let slab_pointer = sent.frames.cells.as_ptr();
        let owners = Arc::strong_count(&sent);
        let (_, counts) = allocation_evidence::measure(|| {
            for _ in 0..256 {
                let mut admission = sent.admit_checked(&payload, 1024).unwrap();
                let mut plan = WriteFramePlan::new();
                stream
                    .append_admitted(&mut plan, admission.frames_receipt())
                    .unwrap();
                admission.commit();
                assert_eq!(
                    stream.prepare_batch(&mut plan, chunks, 0).unwrap(),
                    bytes.len()
                );
                // Exercise the exact successful-byte accounting seam without
                // including executor/socket setup in this allocator scope.
                stream.account_write(bytes).unwrap();
                assert_eq!(plan.fallback_decodes, 0);
                assert_eq!(bytes.as_ptr(), payload_pointer);
            }
        });
        assert_eq!(counts, allocation_evidence::Counts::default());
        assert_eq!(Arc::strong_count(&sent), owners);
        assert_eq!(sent.frames.cells.as_ptr(), slab_pointer);
        assert_eq!(sent.metadata_stats(), (256, 256, 0, 0));
        let (_, positive) = allocation_evidence::measure(|| std::hint::black_box(vec![0u8; 19]));
        assert!(
            positive.allocations > 0,
            "allocator evidence must be nonvacuous"
        );
        let mut fallback = WriteFramePlan::new();
        stream.prepare_batch(&mut fallback, chunks, 0).unwrap();
        assert_eq!(
            fallback.fallback_decodes, 1,
            "decode counter must detect reconstruction"
        );
    }

    #[test]
    fn rollback_captures_link_completed_after_its_old_metadata_snapshot() {
        let store = FrameStore::new(2);
        let entry = StoredFrame {
            frame: InitialOutcome::from_metadata(WireKind::ActorTell, 12, &[])
                .unwrap()
                .0,
            offset: 0,
            bytes: 16,
            next: None,
        };
        let tail = store.insert(entry).unwrap();
        let next = store.insert(entry).unwrap();
        let stale_snapshot = store.get(tail).unwrap();
        assert!(stale_snapshot.next.is_none());
        let mut chain = AdmittedFrames {
            owner: store.owner,
            first: Some(tail),
            last: Some(tail),
        };
        store
            .append(
                &mut chain,
                AdmittedFrames {
                    owner: store.owner,
                    first: Some(next),
                    last: Some(next),
                },
            )
            .unwrap();
        assert_eq!(store.get(tail).unwrap().next, Some(next));
        store.rollback(FrameRollback {
            owner: store.owner,
            first: Some(tail),
        });
        assert_eq!(
            store.free.len(),
            2,
            "retirement must follow the final link, not the old snapshot"
        );
        assert!(store.get(tail).is_err());
        assert!(store.get(next).is_err());
    }

    #[test]
    fn physical_close_reclaims_published_metadata_and_prevents_reopening() {
        let window = TerminalWindow::new();
        let frame = framing::write_actor_tell_header(7, 11, 0);
        let mut admission = window.admit_frames(&frame).unwrap();
        let queued = admission.frames_receipt();
        admission.commit();
        assert_eq!(window.metadata_stats().3, 1);
        window.close();
        assert_eq!(window.metadata_stats().3, 0);
        assert!(matches!(
            window.admit_frames(&frame),
            Err(crate::GossipError::ConnectionDropped)
        ));
        window.frames.release_chain(queued);
        assert_eq!(window.metadata_stats().3, 0);
        let successor = TerminalWindow::new();
        assert_ne!(window.frames.owner, successor.frames.owner);
        assert!(successor.admit_frames(&frame).is_ok());
    }

    #[test]
    fn typed_producer_receipt_survives_partial_attempts_without_reclassification() {
        use crate::ask_forwarder::allocation_evidence;

        let sent = TerminalWindow::new();
        let mut stream = OutcomeStream::new(tokio::io::sink(), sent.clone());
        let header = ask(1);
        let payload = super::super::WritePayload::DirectAskInline {
            header,
            payload: bytes::Bytes::new(),
        };
        let (_, counts) = allocation_evidence::measure(|| {
            let mut admission = sent.admit_checked(&payload, 1024).unwrap();
            assert!(
                !admission.frames.is_empty(),
                "typed producer must retain metadata"
            );
            stream.begin_admitted(admission.frames_receipt()).unwrap();
            admission.commit();
            stream.prepare_frames(&[(&header, 0)]).unwrap();
            stream.account_write(&header[..3]).unwrap();
            stream.prepare_frames(&[(&header, 3)]).unwrap();
            stream.account_write(&header[3..]).unwrap();
        });
        assert_eq!(counts, allocation_evidence::Counts::default());
        assert_eq!(sent.metadata_stats(), (1, 1, 0, 0));
        assert_eq!(occupied(&sent), 1);
        assert_eq!(sent.slots[1].load(Ordering::Acquire), 3);
        stream
            .read
            .as_mut()
            .unwrap()
            .consume(&reply(1), &sent, &stream.received, false)
            .unwrap();
        assert_eq!(occupied(&sent), 0);
        let (_, positive) = allocation_evidence::measure(|| std::hint::black_box(vec![0u8; 19]));
        assert!(positive.allocations > 0);
    }

    #[test]
    fn typed_and_generic_batch_append_retains_original_receipts() {
        let sent = TerminalWindow::new();
        let mut stream = OutcomeStream::new(tokio::io::sink(), sent.clone());
        let header = ask(1);
        let typed = super::super::WritePayload::DirectAskInline {
            header,
            payload: bytes::Bytes::new(),
        };
        let generic_bytes = bytes::Bytes::copy_from_slice(&reply(2));
        let generic = super::super::WritePayload::Single(generic_bytes.clone());
        let mut first = WriteFramePlan::new();
        let mut second = WriteFramePlan::new();
        let mut admission = sent.admit_checked(&typed, 1024).unwrap();
        stream.begin_admitted(admission.frames_receipt()).unwrap();
        admission.commit();
        assert!(stream.append_header(&mut first, &ask(2)).is_err());
        stream.append_header(&mut first, &header).unwrap();
        let mut admission = sent.admit_checked(&generic, 1024).unwrap();
        stream
            .append_admitted(&mut second, admission.frames_receipt())
            .unwrap();
        admission.commit();
        stream.append_plan(&mut first, &mut second).unwrap();
        assert_eq!(second.len, 0);
        let chunks = [bytes::Bytes::copy_from_slice(&header), generic_bytes];
        assert_eq!(stream.prepare_batch(&mut first, &chunks, 0).unwrap(), 32);
        stream.account_write(&header[..5]).unwrap();
        assert!(stream.prepare_frames(&[(&reply(7), 0)]).is_err());
        assert_eq!(stream.prepare_batch(&mut first, &chunks, 5).unwrap(), 27);
        stream.account_write(&header[5..]).unwrap();
        stream.account_write(&reply(2)).unwrap();
        assert_eq!(first.fallback_decodes, 0);
        assert_eq!(sent.metadata_stats(), (2, 2, 0, 0));
        assert_eq!(occupied(&sent), 1);
    }

    #[test]
    fn typed_receipts_reject_foreign_and_recycled_owners() {
        let sent = TerminalWindow::new();
        let foreign = TerminalWindow::new();
        let mut stream = OutcomeStream::new(tokio::io::sink(), sent.clone());
        let typed = super::super::WritePayload::DirectAskInline {
            header: ask(1),
            payload: bytes::Bytes::new(),
        };
        let mut admission = foreign.admit_checked(&typed, 1024).unwrap();
        assert!(stream.begin_admitted(admission.frames_receipt()).is_err());
        drop(admission);
        assert_eq!(occupied(&foreign), 0);
        let mut admission = sent.admit_checked(&typed, 1024).unwrap();
        let receipt = admission.frames_receipt();
        let stale = AdmittedFrames {
            owner: receipt.owner,
            first: receipt.first,
            last: receipt.last,
        };
        stream.begin_admitted(receipt).unwrap();
        admission.commit();
        stream.prepare_frames(&[(&ask(1), 0)]).unwrap();
        stream.account_write(&ask(1)).unwrap();
        stream.begin_admitted(stale).unwrap();
        assert!(stream.prepare_frames(&[(&ask(1), 0)]).is_err());
        assert_eq!(sent.slots[1].load(Ordering::Acquire), 3);
        assert_eq!(sent.metadata_stats().3, 0);
    }

    #[test]
    fn typed_data_saturation_preserves_control_terminal_and_successor_progress() {
        let sent = TerminalWindow::new();
        let header = framing::write_actor_tell_header(7, 11, 0);
        let payload = super::super::WritePayload::HeaderPayload {
            header: bytes::Bytes::copy_from_slice(&header),
            payload: bytes::Bytes::new(),
        };
        let mut held = Vec::with_capacity(FRAME_RECEIPT_CAP);
        for _ in 0..FRAME_RECEIPT_CAP {
            held.push(sent.admit_checked(&payload, 1024).unwrap());
        }
        assert!(matches!(
            sent.admit_checked(&payload, 1024),
            Err(crate::GossipError::WriteQueueFull)
        ));
        let mut stream = OutcomeStream::new(tokio::io::sink(), sent.clone());
        stream.received.reserve(1).unwrap();
        let terminal = super::super::WritePayload::HeaderPayload {
            header: bytes::Bytes::copy_from_slice(&reply(1)),
            payload: bytes::Bytes::new(),
        };
        let mut admission = sent.admit_immediate_checked(&terminal, 1024).unwrap();
        stream.begin_admitted(admission.frames_receipt()).unwrap();
        admission.commit();
        stream.prepare_frames(&[(&reply(1), 0)]).unwrap();
        stream.account_write(&reply(1)[..3]).unwrap();
        assert_eq!(stream.received.occupied(), 1);
        assert!(stream.prepare_frames(&[(&reply(2), 3)]).is_err());
        assert!(stream.prepare_frames(&[(&reply(1), 0)]).is_err());
        stream.prepare_frames(&[(&reply(1), 3)]).unwrap();
        stream.account_write(&reply(1)[3..]).unwrap();
        assert_eq!(stream.received.occupied(), 0);
        let control = framing::write_gossip_frame_prefix(0);
        let payload = super::super::WritePayload::HeaderPayload {
            header: bytes::Bytes::copy_from_slice(&control),
            payload: bytes::Bytes::new(),
        };
        let mut admission = sent.admit_checked(&payload, 1024).unwrap();
        stream.begin_admitted(admission.frames_receipt()).unwrap();
        admission.commit();
        stream.prepare_frames(&[(&control, 0)]).unwrap();
        stream.account_write(&control).unwrap();
        assert_eq!(sent.metadata_stats().3, FRAME_RECEIPT_CAP);
        drop(held);
        assert_eq!(sent.metadata_stats().3, 0);
        let successor = sent.admit_checked(&payload, 1024).unwrap();
        assert_eq!(sent.metadata_stats().3, 1);
        drop(successor);
        assert_eq!(sent.metadata_stats().3, 0);
        assert_eq!(stream.opaque_write_observations(), 0);
    }
}
