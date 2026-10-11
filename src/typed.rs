use crate::{GossipError, Result};
use bytes::{Buf, Bytes};
use crossbeam_queue::ArrayQueue;
use std::alloc::Layout;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

const SERIALIZER_POOL_SIZE: usize = 64;
const BYTE_PAYLOAD_POOL_SIZE: usize = 4096;
const MAX_POOLED_BYTE_CAPACITY: usize = 1024 * 1024; // 1MB
/// Process-wide retained-byte budget for idle pooled payload buffers.
const DEFAULT_RETAINED_BYTE_BUDGET: usize = 64 * 1024 * 1024;

fn validate_archive_alignment<A>(payload: &[u8], type_name: &str) -> Result<()> {
    let required = std::mem::align_of::<A>();
    if required > crate::aligned::PAYLOAD_ALIGNMENT {
        return Err(GossipError::InvalidConfig(format!(
            "typed archive alignment for {type_name} requires {required} bytes, exceeding the {}-byte payload guarantee",
            crate::aligned::PAYLOAD_ALIGNMENT
        )));
    }
    if (payload.as_ptr() as usize) % required != 0 {
        return Err(GossipError::InvalidConfig(format!(
            "typed archive alignment for {type_name} requires a {required}-byte-aligned payload"
        )));
    }
    Ok(())
}

/// Maximum archive body and scratch reservations for each admitted context.
/// These are allocation limits, not post-serialization length checks.
// Leave room for scratch and the type prefix inside the existing response
// lane's retained-owner budget. A larger fixed class would reject tiny replies.
const MAX_TYPED_ARCHIVE_BYTES: usize = 4 * 1024 * 1024;
const MAX_TYPED_SCRATCH_BYTES: usize = 1024 * 1024;
const MAX_TYPED_SHARED_POINTERS: usize = 2048;
const TYPED_SHARING_SLOTS: usize = MAX_TYPED_SHARED_POINTERS * 2;
const MAX_TYPED_SHARING_PROBES: usize = 16;

#[derive(Clone, Copy, Default)]
struct SharingEntry {
    epoch: usize,
    address: usize,
    // usize::MAX is pending; archive positions cannot reach that value.
    position: usize,
}

/// Fixed-capacity shared-pointer unifier. Aliases retain their original archive
/// position and recursive references remain Pending, as with rkyv's Share.
/// New identities beyond 2048, or a full 16-slot collision window, reject the
/// entire encode with WriteQueueFull. Existing aliases in that window survive.
pub struct BoundedArchiveSharing<'a> {
    entries: &'a mut [SharingEntry],
    epoch: usize,
    count: usize,
    rejected: &'a AtomicBool,
}

impl BoundedArchiveSharing<'_> {
    fn slot(&self, address: usize) -> Option<usize> {
        // Pointer alignment must not collapse neighboring allocations into a
        // single bucket. The table is fixed and remains at most half occupied.
        let mixed = (address >> 3).wrapping_mul(0x9e37_79b9);
        let start = mixed % self.entries.len();
        for offset in 0..MAX_TYPED_SHARING_PROBES {
            let index = (start + offset) % self.entries.len();
            let entry = &self.entries[index];
            if entry.epoch != self.epoch || entry.address == address {
                return Some(index);
            }
        }
        None
    }
}

impl<E: rkyv::rancor::Source> rkyv::ser::Sharing<E> for BoundedArchiveSharing<'_> {
    fn start_sharing(&mut self, address: usize) -> rkyv::ser::sharing::SharingState {
        use rkyv::ser::sharing::SharingState;
        let Some(index) = self.slot(address) else {
            self.rejected.store(true, Ordering::Relaxed);
            return SharingState::Pending;
        };
        let entry = &mut self.entries[index];
        if entry.epoch == self.epoch {
            return if entry.position == usize::MAX {
                SharingState::Pending
            } else {
                SharingState::Finished(entry.position)
            };
        }
        if self.count == MAX_TYPED_SHARED_POINTERS {
            self.rejected.store(true, Ordering::Relaxed);
            // Sharing has no fallible start method. Pending makes the standard
            // extension stop, while the sticky flag preserves typed admission
            // failure even when a custom serializer swallows that error.
            return SharingState::Pending;
        }
        *entry = SharingEntry {
            epoch: self.epoch,
            address,
            position: usize::MAX,
        };
        self.count += 1;
        SharingState::Started
    }

    fn finish_sharing(&mut self, address: usize, pos: usize) -> std::result::Result<(), E> {
        let Some(index) = self.slot(address) else {
            self.rejected.store(true, Ordering::Relaxed);
            return Err(E::new(TypedCapacityExceeded));
        };
        let entry = &mut self.entries[index];
        if entry.epoch != self.epoch || entry.position != usize::MAX || pos == usize::MAX {
            self.rejected.store(true, Ordering::Relaxed);
            return Err(E::new(TypedCapacityExceeded));
        }
        entry.position = pos;
        Ok(())
    }
}

#[derive(Debug)]
struct TypedCapacityExceeded;

impl std::fmt::Display for TypedCapacityExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("typed serializer capacity exhausted")
    }
}

impl std::error::Error for TypedCapacityExceeded {}

/// Fixed-reservation rkyv writer. The mutable storage is deliberately private:
/// custom serializers cannot obtain a growable vector through this adapter.
pub struct BoundedArchiveWriter<'a> {
    buffer: &'a mut rkyv::util::AlignedVec,
    rejected: &'a AtomicBool,
    observed_oversize: &'a AtomicUsize,
}

impl rkyv::ser::Positional for BoundedArchiveWriter<'_> {
    fn pos(&self) -> usize {
        self.buffer.len()
    }
}

impl BoundedArchiveWriter<'_> {
    fn check_write_len<E: rkyv::rancor::Source>(&self, len: usize) -> std::result::Result<(), E> {
        let attempted = self.buffer.len().saturating_add(len);
        if attempted > crate::MAX_STREAM_SIZE {
            // The offered slice already proves the wire hard limit is exceeded.
            // Preserve this diagnosis without materializing or growing it.
            self.observed_oversize.store(attempted, Ordering::Relaxed);
            self.rejected.store(true, Ordering::Relaxed);
            return Err(E::new(TypedCapacityExceeded));
        }
        if len > self.buffer.capacity().saturating_sub(self.buffer.len()) {
            self.rejected.store(true, Ordering::Relaxed);
            return Err(E::new(TypedCapacityExceeded));
        }
        Ok(())
    }
}

impl<E: rkyv::rancor::Source> rkyv::ser::Writer<E> for BoundedArchiveWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::result::Result<(), E> {
        self.check_write_len::<E>(bytes.len())?;
        // Capacity was reserved before user serialization began. This cannot
        // call the allocation/growth path, including for padding writes.
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }
}

/// Fixed scratch slab. All live allocations are disjoint ranges in this slab;
/// alignment padding is charged before exposing a pointer. Nothing reallocates
/// while a serializer holds scratch pointers. A single raw base pointer is
/// derived at construction: later pushes do not reborrow the entire slab and
/// invalidate pointers belonging to earlier live scratch allocations.
pub struct BoundedArchiveAllocator<'a> {
    base: NonNull<u8>,
    len: usize,
    cursor: usize,
    rejected: &'a AtomicBool,
    borrow: PhantomData<&'a mut [u8]>,
}

impl<'a> BoundedArchiveAllocator<'a> {
    fn new(slab: &'a mut [u8], rejected: &'a AtomicBool) -> Self {
        let len = slab.len();
        let base = NonNull::new(slab.as_mut_ptr()).expect("slice pointers are nonnull");
        Self {
            base,
            len,
            cursor: 0,
            rejected,
            borrow: PhantomData,
        }
    }
}

// SAFETY: push returns nonzero, correctly aligned, exclusive ranges within a
// fixed live allocation. The Allocator caller guarantees a matching layout and
// that no allocations above a popped allocation remain in use. pop reclaims
// only that allocation and those above it; the slab outlives the serializer.
unsafe impl<E: rkyv::rancor::Source> rkyv::ser::Allocator<E> for BoundedArchiveAllocator<'_> {
    unsafe fn push_alloc(&mut self, layout: Layout) -> std::result::Result<NonNull<[u8]>, E> {
        let base = self.base.as_ptr() as usize;
        let start = base
            .checked_add(self.cursor)
            .and_then(|address| address.checked_add(layout.align() - 1))
            .map(|address| (address & !(layout.align() - 1)) - base);
        let end = start.and_then(|start| start.checked_add(layout.size()));
        let Some((start, end)) = start.zip(end).filter(|(_, end)| *end <= self.len) else {
            self.rejected.store(true, Ordering::Relaxed);
            return Err(E::new(TypedCapacityExceeded));
        };
        // SAFETY: the checked range is inside the exclusively borrowed slab.
        // Allocator::push_alloc requires a nonzero layout from its caller.
        let pointer = unsafe { NonNull::new_unchecked(self.base.as_ptr().add(start)) };
        self.cursor = end;
        Ok(NonNull::slice_from_raw_parts(pointer, layout.size()))
    }

    unsafe fn pop_alloc(
        &mut self,
        pointer: NonNull<u8>,
        _layout: Layout,
    ) -> std::result::Result<(), E> {
        // SAFETY: the caller guarantees this pointer came from push_alloc on
        // this allocator and all later live allocations have been popped.
        self.cursor = unsafe { pointer.as_ptr().offset_from(self.base.as_ptr()) } as usize;
        Ok(())
    }
}

struct SerializerCtx {
    writer: rkyv::util::AlignedVec,
    arena: rkyv::util::AlignedVec,
    sharing: Box<[SharingEntry]>,
    sharing_epoch: usize,
    rejected: AtomicBool,
    observed_oversize: AtomicUsize,
}

impl SerializerCtx {
    fn new() -> Self {
        Self {
            writer: rkyv::util::AlignedVec::new(),
            arena: rkyv::util::AlignedVec::new(),
            sharing: Box::default(),
            sharing_epoch: 0,
            rejected: AtomicBool::new(false),
            observed_oversize: AtomicUsize::new(0),
        }
    }

    fn prepare(&mut self) {
        // Context count admission precedes both allocations. Each has exactly
        // one configured reservation and never grows during serialization.
        if self.writer.capacity() == 0 {
            self.writer = rkyv::util::AlignedVec::with_capacity(MAX_TYPED_ARCHIVE_BYTES);
        }
        if self.arena.capacity() == 0 {
            self.arena = rkyv::util::AlignedVec::with_capacity(MAX_TYPED_SCRATCH_BYTES);
            self.arena.resize(MAX_TYPED_SCRATCH_BYTES, 0);
        }
        if self.sharing.is_empty() {
            self.sharing = vec![SharingEntry::default(); TYPED_SHARING_SLOTS].into_boxed_slice();
        }
        self.sharing_epoch = self.sharing_epoch.wrapping_add(1);
        if self.sharing_epoch == 0 {
            self.sharing.fill(SharingEntry::default());
            self.sharing_epoch = 1;
        }
        self.writer.clear();
        self.rejected.store(false, Ordering::Relaxed);
        self.observed_oversize.store(0, Ordering::Relaxed);
    }
}

static SERIALIZER_POOL: OnceLock<ArrayQueue<Box<SerializerCtx>>> = OnceLock::new();

fn serializer_pool() -> &'static ArrayQueue<Box<SerializerCtx>> {
    SERIALIZER_POOL.get_or_init(|| {
        let pool = ArrayQueue::new(SERIALIZER_POOL_SIZE);
        for _ in 0..SERIALIZER_POOL_SIZE {
            let _ = pool.push(Box::new(SerializerCtx::new()));
        }
        pool
    })
}

const MAX_BYTE_PAYLOAD_SIZE: usize = 8 * 1024 * 1024;
const CHECKED_OUT_BYTE_BUDGET: usize = 64 * 1024 * 1024;
static CHECKED_OUT_BYTES: AtomicUsize = AtomicUsize::new(0);

/// Bounded encoder destination. Writes exceeding the admitted length fail the
/// whole encode with `WriteQueueFull`; no growth or partial payload escapes.
pub struct BoundedByteWriter {
    buffer: Vec<u8>,
    limit: usize,
    overflow: bool,
    charge: usize,
}

impl BoundedByteWriter {
    pub fn extend_from_slice(&mut self, bytes: &[u8]) {
        if bytes.len() > self.limit.saturating_sub(self.buffer.len()) {
            self.overflow = true;
        } else {
            self.buffer.extend_from_slice(bytes);
        }
    }

    pub fn push(&mut self, byte: u8) {
        self.extend_from_slice(&[byte]);
    }

    pub fn resize(&mut self, len: usize, value: u8) {
        if len > self.limit {
            self.overflow = true;
        } else {
            self.buffer.resize(len, value);
        }
    }
}

impl Extend<u8> for BoundedByteWriter {
    fn extend<I: IntoIterator<Item = u8>>(&mut self, iter: I) {
        for byte in iter {
            if self.buffer.len() == self.limit {
                self.overflow = true;
                break;
            }
            self.buffer.push(byte);
        }
    }
}

impl Drop for BoundedByteWriter {
    fn drop(&mut self) {
        release_byte_buffer(std::mem::take(&mut self.buffer));
        CHECKED_OUT_BYTES.fetch_sub(self.charge, Ordering::AcqRel);
    }
}

struct BytePayloadPool {
    queue: ArrayQueue<Vec<u8>>,
    retained_bytes: AtomicUsize,
    /// Encodes that exceeded the pooled retention bound and were served by a
    /// fresh unpooled allocation. Exposed for diagnostics: an oversize encode
    /// used to fail outright (the frame was silently dropped), so the count
    /// marks a path that previously lost data.
    oversize_checkouts: AtomicUsize,
    budget: usize,
}

impl BytePayloadPool {
    fn new(slots: usize, budget: usize) -> Self {
        Self {
            queue: ArrayQueue::new(slots),
            retained_bytes: AtomicUsize::new(0),
            oversize_checkouts: AtomicUsize::new(0),
            budget,
        }
    }

    fn retained_bytes(&self) -> usize {
        self.retained_bytes.load(Ordering::Acquire)
    }

    fn try_reserve(&self, bytes: usize) -> bool {
        self.retained_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |current| {
                current
                    .checked_add(bytes)
                    .filter(|next| *next <= self.budget)
            })
            .is_ok()
    }

    fn release_reservation(&self, bytes: usize) {
        self.retained_bytes.fetch_sub(bytes, Ordering::AcqRel);
    }

    fn acquire(&self, min_capacity: usize) -> Option<Vec<u8>> {
        if min_capacity > MAX_POOLED_BYTE_CAPACITY {
            return None;
        }
        match self.queue.pop() {
            Some(buffer) if buffer.capacity() >= min_capacity => {
                self.release_reservation(buffer.capacity());
                Some(buffer)
            }
            Some(buffer) => {
                self.release_reservation(buffer.capacity());
                drop(buffer);
                Some(Vec::with_capacity(min_capacity))
            }
            None => Some(Vec::with_capacity(min_capacity)),
        }
    }

    fn release(&self, mut buffer: Vec<u8>) {
        buffer.clear();
        let cap = buffer.capacity();
        if cap > MAX_POOLED_BYTE_CAPACITY {
            return;
        }
        if !self.try_reserve(cap) {
            return;
        }
        if self.queue.push(buffer).is_err() {
            self.release_reservation(cap);
        }
    }

    fn prewarm(&self, count: usize, capacity: usize) {
        if capacity > MAX_POOLED_BYTE_CAPACITY {
            return;
        }
        for _ in 0..count {
            if self.queue.is_full() {
                return;
            }
            let buffer = Vec::with_capacity(capacity);
            let cap = buffer.capacity();
            if cap > MAX_POOLED_BYTE_CAPACITY || !self.try_reserve(cap) {
                return;
            }
            if self.queue.push(buffer).is_err() {
                self.release_reservation(cap);
                return;
            }
        }
    }
}

static BYTE_PAYLOAD_POOL: OnceLock<BytePayloadPool> = OnceLock::new();

fn byte_payload_pool() -> &'static BytePayloadPool {
    BYTE_PAYLOAD_POOL
        .get_or_init(|| BytePayloadPool::new(BYTE_PAYLOAD_POOL_SIZE, DEFAULT_RETAINED_BYTE_BUDGET))
}

/// Idle pooled payload capacity currently retained by the process-wide pool.
/// Checked-out buffers and the bounded global serializer contexts are not included.
pub fn retained_byte_pool_bytes() -> usize {
    byte_payload_pool().retained_bytes()
}

/// Count of payload encodes that exceeded the pool's retention bound and
/// attempted an unpooled allocation. Nonzero is legal — it marks frames the
/// old admission-as-pool design would have dropped silently — but a steadily
/// climbing value means a hot path is serializing payloads larger than the
/// pool is tuned for.
pub fn oversize_byte_pool_checkouts() -> usize {
    byte_payload_pool()
        .oversize_checkouts
        .load(Ordering::Acquire)
}

fn acquire_ctx_from(pool: &ArrayQueue<Box<SerializerCtx>>) -> Result<Box<SerializerCtx>> {
    pool.pop().ok_or(GossipError::WriteQueueFull)
}

fn acquire_ctx() -> Result<Box<SerializerCtx>> {
    acquire_ctx_from(serializer_pool())
}

fn release_ctx(mut ctx: Box<SerializerCtx>) {
    ctx.writer.clear();
    ctx.rejected.store(false, Ordering::Relaxed);
    // Keep fixed reservations for allocation-free reuse. There is no arena
    // growth/shrink path and no unbounded oversized idle context.
    let _ = serializer_pool().push(ctx);
}

fn try_acquire_byte_buffer(min_capacity: usize) -> Result<BoundedByteWriter> {
    if min_capacity > MAX_BYTE_PAYLOAD_SIZE {
        return Err(GossipError::WriteQueueFull);
    }
    // A reused small buffer may retain up to the idle class maximum. Reserve
    // that class before popping or allocating, so hidden surplus is charged.
    let charge = min_capacity
        .max(MAX_POOLED_BYTE_CAPACITY)
        .next_power_of_two();
    CHECKED_OUT_BYTES
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current
                .checked_add(charge)
                .filter(|next| *next <= CHECKED_OUT_BYTE_BUDGET)
        })
        .map_err(|_| GossipError::WriteQueueFull)?;
    let buffer = if min_capacity <= MAX_POOLED_BYTE_CAPACITY {
        byte_payload_pool().acquire(min_capacity)
    } else {
        byte_payload_pool()
            .oversize_checkouts
            .fetch_add(1, Ordering::Relaxed);
        let mut buffer = Vec::new();
        match buffer.try_reserve_exact(min_capacity) {
            Ok(()) => Some(buffer),
            Err(_) => None,
        }
    };
    let Some(mut buffer) = buffer else {
        CHECKED_OUT_BYTES.fetch_sub(charge, Ordering::AcqRel);
        return Err(GossipError::WriteQueueFull);
    };
    if buffer.capacity() > charge {
        drop(buffer);
        CHECKED_OUT_BYTES.fetch_sub(charge, Ordering::AcqRel);
        return Err(GossipError::WriteQueueFull);
    }
    buffer.clear();
    Ok(BoundedByteWriter {
        buffer,
        limit: min_capacity,
        overflow: false,
        charge,
    })
}

fn release_byte_buffer(buffer: Vec<u8>) {
    byte_payload_pool().release(buffer);
}

pub(crate) fn prewarm_pooled_byte_buffers(count: usize, capacity: usize) {
    byte_payload_pool().prewarm(count, capacity);
}

#[cfg(test)]
mod qa_pool_review {
    use super::*;
    use bytes::Buf;
    use std::sync::Arc;

    #[test]
    fn concurrent_byte_checkout_does_not_report_spurious_capacity_exhaustion() {
        for _ in 0..8 {
            let start = Arc::new(std::sync::Barrier::new(17));
            let workers: Vec<_> = (0..16)
                .map(|_| {
                    let start = start.clone();
                    std::thread::spawn(move || {
                        start.wait();
                        try_acquire_byte_buffer(32)
                    })
                })
                .collect();
            start.wait();
            let held: Vec<_> = workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .collect();
            assert!(
                held.iter().all(Result::is_ok),
                "16 one-MiB reservations fit the unchanged 64-MiB byte budget"
            );
            assert!(CHECKED_OUT_BYTES.load(Ordering::Acquire) <= CHECKED_OUT_BYTE_BUDGET);
            drop(held);
        }
    }

    #[test]
    fn qa_prewarmed_pool_adapts_to_larger_steady_state_payloads() {
        let pool = BytePayloadPool::new(64, DEFAULT_RETAINED_BYTE_BUDGET);
        pool.prewarm(64, 4096);
        for _ in 0..100 {
            let buffer = pool.acquire(8192).unwrap();
            pool.release(buffer);
        }
        let mut suitable = 0;
        let mut undersized = 0;
        while let Some(buffer) = pool.queue.pop() {
            pool.release_reservation(buffer.capacity());
            if buffer.capacity() >= 8192 {
                suitable += 1;
            } else {
                undersized += 1;
            }
        }
        assert!(
            suitable > 0,
            "larger steady-state messages must become reusable instead of allocating forever (suitable={suitable}, undersized={undersized})"
        );
        assert_eq!(pool.retained_bytes(), 0);
    }

    #[test]
    fn into_remaining_bytes_preserves_payload_and_offset() {
        let mut payload = PooledPayload::try_from_pooled_bytes(8, |buf| {
            buf.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        })
        .unwrap();
        payload.advance(3);
        let bytes = payload.into_remaining_bytes();
        assert_eq!(&bytes[..], &[4, 5, 6, 7, 8]);
    }

    #[test]
    fn retained_bytes_use_capacity_not_length() {
        let budget = 3 * 1024;
        let pool = BytePayloadPool::new(8, budget);
        let mut first = pool.acquire(2048).unwrap();
        first.resize(16, 1);
        let mut second = pool.acquire(2048).unwrap();
        second.resize(16, 2);
        assert_eq!(
            pool.retained_bytes(),
            0,
            "checked-out buffers are not retained"
        );
        pool.release(first);
        pool.release(second);
        assert!(pool.retained_bytes() <= budget);
        assert!(
            pool.retained_bytes() >= 2048,
            "idle pool must account for capacity bytes, not payload lengths (retained={})",
            pool.retained_bytes()
        );
        // A count-only policy would keep both 2 KiB-class buffers. The byte
        // budget must drop at least one so retained capacity stays in bound.
        assert!(
            pool.queue.len() < 2 || pool.retained_bytes() <= budget,
            "byte budget must reject a second buffer that would exceed retained capacity"
        );
    }

    #[test]
    fn small_budget_drops_the_second_large_return() {
        let budget = 1536;
        let pool = BytePayloadPool::new(8, budget);
        let a = Vec::with_capacity(1024);
        let cap = a.capacity();
        assert!(
            cap <= budget,
            "fixture capacity {cap} must fit the {budget} byte budget once"
        );
        let b = Vec::with_capacity(1024);
        pool.release(a);
        pool.release(b);
        assert!(pool.retained_bytes() <= budget);
        assert_eq!(pool.queue.len(), 1);
        assert_eq!(pool.retained_bytes(), cap);
    }

    #[test]
    fn oversize_buffer_is_dropped_without_retaining() {
        let pool = BytePayloadPool::new(8, DEFAULT_RETAINED_BYTE_BUDGET);
        let oversize = Vec::with_capacity(MAX_POOLED_BYTE_CAPACITY + 1);
        pool.release(oversize);
        assert_eq!(pool.retained_bytes(), 0);
        assert_eq!(pool.queue.len(), 0);
        assert!(pool.acquire(MAX_POOLED_BYTE_CAPACITY + 1).is_none());
    }

    #[test]
    fn oversize_payload_encodes_on_an_unpooled_buffer() {
        // The pool bound is a *retention* bound: it caps the size of idle
        // buffer the pool will hold, not the size of payload the codec may
        // encode. An oversized encode must still succeed on a fresh
        // allocation — silently dropping the frame is how a >1 MiB pubsub
        // message vanished between a healthy publisher and its subscriber.
        let size = MAX_POOLED_BYTE_CAPACITY + 128;
        let payload = PooledPayload::try_from_pooled_bytes(size, |out| {
            out.extend(std::iter::repeat_n(7u8, size));
        })
        .expect("oversize payload encodes on an unpooled buffer");
        assert_eq!(payload.len(), size);
    }

    #[test]
    fn oversized_capacity_request_returns_none_without_running_fill() {
        let mut fill_called = false;
        let payload = PooledPayload::try_from_pooled_bytes(usize::MAX, |_| {
            fill_called = true;
        });
        assert!(payload.is_err());
        assert!(!fill_called);
    }

    #[test]
    fn checkout_releases_reservation_exactly_once() {
        let pool = BytePayloadPool::new(8, 4096);
        let buffer = Vec::with_capacity(1024);
        let cap = buffer.capacity();
        pool.release(buffer);
        assert_eq!(pool.retained_bytes(), cap);
        let checked_out = pool.acquire(1024).unwrap();
        assert_eq!(pool.retained_bytes(), 0);
        let cap = checked_out.capacity();
        pool.release(checked_out);
        assert_eq!(pool.retained_bytes(), cap);
    }

    #[test]
    fn concurrent_returns_never_exceed_budget_or_underflow() {
        let budget = 8 * 1024;
        let pool = Arc::new(BytePayloadPool::new(32, budget));
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let pool = Arc::clone(&pool);
                scope.spawn(move || {
                    for _ in 0..64 {
                        let buffer = pool.acquire(1024).unwrap();
                        assert!(buffer.capacity() >= 1024);
                        pool.release(buffer);
                        assert!(pool.retained_bytes() <= budget);
                    }
                });
            }
        });
        assert!(pool.retained_bytes() <= budget);
    }

    #[test]
    fn large_then_small_payloads_reuse_without_exceeding_budget() {
        let budget = 4 * 1024;
        let pool = BytePayloadPool::new(8, budget);
        let large = pool.acquire(2048).unwrap();
        pool.release(large);
        let small = pool.acquire(64).unwrap();
        assert!(small.capacity() >= 64);
        pool.release(small);
        assert!(pool.retained_bytes() <= budget);
        assert!(pool.retained_bytes() > 0);
    }
}

fn encode_typed_in<T>(value: &T, ctx: &mut SerializerCtx) -> Result<usize>
where
    T: WireEncode,
{
    ctx.prepare();
    let mut serializer = rkyv::ser::Serializer::new(
        BoundedArchiveWriter {
            buffer: &mut ctx.writer,
            rejected: &ctx.rejected,
            observed_oversize: &ctx.observed_oversize,
        },
        BoundedArchiveAllocator::new(&mut ctx.arena, &ctx.rejected),
        BoundedArchiveSharing {
            entries: &mut ctx.sharing,
            epoch: ctx.sharing_epoch,
            count: 0,
            rejected: &ctx.rejected,
        },
    );
    let result = rkyv::api::serialize_using::<_, rkyv::rancor::Error>(value, &mut serializer);
    let oversized = ctx.observed_oversize.load(Ordering::Relaxed);
    if oversized != 0 {
        return Err(GossipError::MessageTooLarge {
            size: oversized,
            max: crate::MAX_STREAM_SIZE,
        });
    }
    // A custom Serialize implementation may swallow a Writer/Allocator error.
    // The sticky receipt still rejects the entire archive in that case.
    if ctx.rejected.load(Ordering::Relaxed) {
        return Err(GossipError::WriteQueueFull);
    }
    result.map_err(GossipError::Serialization)?;
    Ok(ctx.writer.len())
}

/// Pooled payload that implements bytes::Buf without copying.
///
/// Byte encodes admit at most 8 MiB each and charge at least a 1 MiB reuse
/// class against a process-wide 64 MiB live budget (at most 64 checkouts).
/// Idle byte storage has a separate 64 MiB retention budget. Saturation or
/// concurrent reservation contention returns `WriteQueueFull` before filling.
/// Ownership, including this admission charge, persists through every Bytes
/// view until the last owner drops. There is no forced recycling of borrowers.
/// Serializer contexts have 64 global checkouts, each with a fixed 4 MiB
/// archive reservation and 1 MiB scratch reservation. Their storage remains
/// owned by the context through serialization, errors, unwinding, and views.
/// Sharing admits 2048 distinct pointers in 4096 fixed slots per context;
/// slot bytes are included in retained_bytes. VectorClock entry traversal has
/// a separate 8 MiB process-wide admission budget and retains no copied map.
/// Pool storage is bounded by 64 times these three reservations, plus fixed
/// context/queue metadata. Custom serializers must use these adapters rather
/// than allocate private buffers.
pub struct PooledPayload {
    inner: Option<PooledPayloadInner>,
    len: usize,
    pos: usize,
}

enum PooledPayloadInner {
    Serializer(Box<SerializerCtx>),
    Bytes(BoundedByteWriter),
    Owned(crate::ask_forwarder::ForwardPayload),
}

/// Concrete pooled storage transferred without a `Bytes::from_owner` wrapper.
/// This deliberately excludes PooledPayloadInner::Owned, so embedding it in a
/// ForwardPayload cannot create a recursive owner representation.
pub(crate) struct NativePooledOwner {
    inner: Option<NativePooledStorage>,
}

enum NativePooledStorage {
    Serializer(Box<SerializerCtx>),
    Bytes(BoundedByteWriter),
}

impl NativePooledOwner {
    pub(crate) fn retained_bytes(&self) -> usize {
        match self.inner.as_ref() {
            Some(NativePooledStorage::Serializer(ctx)) => ctx
                .writer
                .capacity()
                .saturating_add(ctx.arena.capacity())
                .saturating_add(std::mem::size_of_val(ctx.sharing.as_ref())),
            Some(NativePooledStorage::Bytes(bytes)) => bytes.buffer.capacity(),
            None => 0,
        }
    }
}

impl AsRef<[u8]> for NativePooledOwner {
    fn as_ref(&self) -> &[u8] {
        match self.inner.as_ref() {
            Some(NativePooledStorage::Serializer(ctx)) => ctx.writer.as_ref(),
            Some(NativePooledStorage::Bytes(bytes)) => bytes.buffer.as_ref(),
            None => &[],
        }
    }
}

impl Drop for NativePooledOwner {
    fn drop(&mut self) {
        match self.inner.take() {
            Some(NativePooledStorage::Serializer(ctx)) => release_ctx(ctx),
            Some(NativePooledStorage::Bytes(bytes)) => drop(bytes),
            None => {}
        }
    }
}

impl PooledPayload {
    /// Preserve an already-accounted whole owner without copying its body.
    /// This transfer adds no pool admission; the enqueue path must reserve its
    /// own count and retained-byte budget before retaining the payload.
    pub fn from_owned(payload: crate::ask_forwarder::ForwardPayload) -> Self {
        let len = payload.len();
        Self {
            inner: Some(PooledPayloadInner::Owned(payload)),
            len,
            pos: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn try_from_pooled_bytes(
        min_capacity: usize,
        fill: impl FnOnce(&mut BoundedByteWriter),
    ) -> Result<Self> {
        let mut buffer = try_acquire_byte_buffer(min_capacity)?;
        fill(&mut buffer);
        if buffer.overflow {
            return Err(GossipError::WriteQueueFull);
        }
        let len = buffer.buffer.len();
        Ok(Self {
            inner: Some(PooledPayloadInner::Bytes(buffer)),
            len,
            pos: 0,
        })
    }

    /// Move the remaining view and its complete native owner into forwarding.
    /// Existing owned wrappers are flattened, so repeated roundtrips add no
    /// allocation, sharing, or owner nesting.
    pub(crate) fn into_forward_payload(mut self) -> crate::ForwardPayload {
        let offset = self.pos;
        let len = self.len.saturating_sub(offset);
        let inner = match self.inner.take() {
            Some(PooledPayloadInner::Serializer(ctx)) => NativePooledStorage::Serializer(ctx),
            Some(PooledPayloadInner::Bytes(bytes)) => NativePooledStorage::Bytes(bytes),
            Some(PooledPayloadInner::Owned(payload)) => {
                return payload.remaining_view(offset, len);
            }
            None => return crate::ForwardPayload::from_static(&[]),
        };
        crate::ForwardPayload::from_native_pooled(
            NativePooledOwner { inner: Some(inner) },
            offset,
            len,
        )
    }

    /// Take remaining bytes out of the pool wrapper. The allocation is held
    /// by the returned `Bytes` until write completion instead of being
    /// returned to the pool immediately.
    #[cfg(test)]
    pub(crate) fn into_remaining_bytes(self) -> Bytes {
        Bytes::from_owner(self)
    }

    /// Actual retained owner capacity, including serializer scratch storage.
    pub fn retained_bytes(&self) -> usize {
        match self.inner.as_ref() {
            Some(PooledPayloadInner::Serializer(ctx)) => ctx
                .writer
                .capacity()
                .saturating_add(ctx.arena.capacity())
                .saturating_add(std::mem::size_of_val(ctx.sharing.as_ref())),
            Some(PooledPayloadInner::Bytes(bytes)) => bytes.buffer.capacity(),
            Some(PooledPayloadInner::Owned(payload)) => payload.retained_bytes(),
            None => 0,
        }
    }
}

impl AsRef<[u8]> for PooledPayload {
    fn as_ref(&self) -> &[u8] {
        self.chunk()
    }
}

impl Buf for PooledPayload {
    fn remaining(&self) -> usize {
        self.len.saturating_sub(self.pos)
    }

    fn chunk(&self) -> &[u8] {
        match self.inner.as_ref() {
            Some(PooledPayloadInner::Serializer(ctx)) => &ctx.writer[self.pos..self.len],
            Some(PooledPayloadInner::Bytes(buffer)) => &buffer.buffer[self.pos..self.len],
            Some(PooledPayloadInner::Owned(payload)) => &payload.as_ref()[self.pos..self.len],
            None => &[],
        }
    }

    fn advance(&mut self, cnt: usize) {
        let remaining = self.remaining();
        assert!(
            cnt <= remaining,
            "cannot advance past remaining bytes: requested {cnt}, remaining {remaining}"
        );
        self.pos += cnt;
    }
}

impl Drop for PooledPayload {
    fn drop(&mut self) {
        match self.inner.take() {
            Some(PooledPayloadInner::Serializer(ctx)) => release_ctx(ctx),
            Some(PooledPayloadInner::Bytes(buffer)) => drop(buffer),
            Some(PooledPayloadInner::Owned(payload)) => drop(payload),
            None => {}
        }
    }
}

/// Compile-time wire type marker with a stable hash identifier.
///
/// The hash should be derived from a stable, shared identifier (e.g. a protocol name)
/// so different binaries can agree on the same type mapping.
pub trait WireType {
    const TYPE_HASH: u64;
    const TYPE_NAME: &'static str;
}

/// Serializer contract used by typed wire encoding. Custom implementations
/// must allocate archive/scratch storage only through these bounded adapters;
/// derived rkyv implementations require no source changes.
pub type BoundedSerializer<'a> = rkyv::rancor::Strategy<
    rkyv::ser::Serializer<
        BoundedArchiveWriter<'a>,
        BoundedArchiveAllocator<'a>,
        BoundedArchiveSharing<'a>,
    >,
    rkyv::rancor::Error,
>;

/// Helper trait for rkyv-serializable wire types.
pub trait WireEncode: WireType + for<'a> rkyv::Serialize<BoundedSerializer<'a>> {}

impl<T> WireEncode for T where T: WireType + for<'a> rkyv::Serialize<BoundedSerializer<'a>> {}

/// Helper trait for rkyv-deserializable wire types.
pub trait WireDecode: WireType + rkyv::Archive + Sized
where
    for<'a> <Self as rkyv::Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        > + rkyv::Deserialize<Self, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
{
}

impl<T> WireDecode for T
where
    T: WireType + rkyv::Archive,
    for<'a> T::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        > + rkyv::Deserialize<T, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
{
}

/// FNV-1a 64-bit hash for stable compile-time hashing of string literals.
pub const fn fnv1a_hash(input: &str) -> u64 {
    let bytes = input.as_bytes();
    let mut hash: u64 = 0xcbf29ce484222325;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(0x100000001b3);
        i += 1;
    }
    hash
}

/// Encode a typed message for the wire.
///
/// Prefixes the payload with the type hash for validation. This is
/// unconditional in every build mode: debug and release must speak the same
/// wire format for typed payloads, or a mixed debug/release deployment would
/// silently corrupt or misinterpret them.
///
/// Important: the prefix is padded to preserve alignment for total zero-copy archived access
/// on the receiver. (See `PAYLOAD_ALIGNMENT` in `aligned.rs`.)
pub fn encode_typed<T>(value: &T) -> Result<Bytes>
where
    T: WireEncode,
{
    encode_typed_owned(value).map(crate::ForwardPayload::into_bytes)
}

/// Copying convenience encoder with an explicit whole-allocation receipt.
/// The wire prefix and strict zero-copy feature rejection match `encode_typed`.
pub(crate) fn encode_typed_owned<T>(value: &T) -> Result<crate::ForwardPayload>
where
    T: WireEncode,
{
    crate::reject_non_zero_copy_path("typed::encode_typed (copying encoder)")?;
    let payload = encode_typed_pooled(value)?;

    const PREFIX_LEN: usize = 16;
    let mut buf = Vec::with_capacity(PREFIX_LEN + payload.len());
    buf.extend_from_slice(&T::TYPE_HASH.to_be_bytes());
    // Pad to 16 bytes so the archive body stays 16-aligned when the underlying buffer is.
    buf.extend_from_slice(&[0u8; PREFIX_LEN - 8]);
    buf.extend_from_slice(payload.as_ref());
    Ok(crate::ForwardPayload::from(buf))
}

/// Encode a typed payload using the pooled serializer context.
pub fn encode_typed_pooled<T>(value: &T) -> Result<PooledPayload>
where
    T: WireEncode,
{
    let ctx = acquire_ctx()?;
    let mut payload = PooledPayload {
        inner: Some(PooledPayloadInner::Serializer(ctx)),
        len: 0,
        pos: 0,
    };
    // Install the owner before invoking user serialization. Error and panic
    // both return its checkout; neither permanently consumes a pool slot.
    if let Some(PooledPayloadInner::Serializer(ctx)) = payload.inner.as_mut() {
        payload.len = encode_typed_in(value, ctx)?;
    }
    Ok(payload)
}

/// Wrap a pooled payload with the type hash prefix. Unconditional in every
/// build mode -- see `encode_typed`.
pub fn typed_payload_parts<T: WireType>(
    payload: PooledPayload,
) -> (PooledPayload, Option<[u8; 16]>, usize) {
    const PREFIX_LEN: usize = 16;
    let mut prefix = [0u8; PREFIX_LEN];
    prefix[..8].copy_from_slice(&T::TYPE_HASH.to_be_bytes());
    let total_len = prefix.len() + payload.len();
    (payload, Some(prefix), total_len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aligned::{AlignedBytes, AlignedBytesPool};
    use crate::wire_type;
    use std::sync::Arc;

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
    struct TestMsg {
        value: u64,
    }

    wire_type!(TestMsg, "typed::TestMsg");

    #[test]
    fn sharing_preserves_alias_cycles_saturation_and_epoch_reuse() {
        use rkyv::ser::sharing::SharingState;
        type Sharing<'a> = BoundedArchiveSharing<'a>;
        let mut ctx = SerializerCtx::new();
        ctx.prepare();
        let pointer = ctx.sharing.as_ptr();
        let mut sharing = BoundedArchiveSharing {
            entries: &mut ctx.sharing,
            epoch: ctx.sharing_epoch,
            count: 0,
            rejected: &ctx.rejected,
        };
        assert!(matches!(
            <Sharing as rkyv::ser::Sharing<rkyv::rancor::Error>>::start_sharing(&mut sharing, 0),
            SharingState::Started
        ));
        assert!(matches!(
            <Sharing as rkyv::ser::Sharing<rkyv::rancor::Error>>::start_sharing(&mut sharing, 0),
            SharingState::Pending
        ));
        <Sharing as rkyv::ser::Sharing<rkyv::rancor::Error>>::finish_sharing(&mut sharing, 0, 17)
            .unwrap();
        for address in 1..MAX_TYPED_SHARED_POINTERS {
            assert!(matches!(
                <Sharing as rkyv::ser::Sharing<rkyv::rancor::Error>>::start_sharing(
                    &mut sharing,
                    address
                ),
                SharingState::Started
            ));
            <Sharing as rkyv::ser::Sharing<rkyv::rancor::Error>>::finish_sharing(
                &mut sharing,
                address,
                address,
            )
            .unwrap();
        }
        assert!(matches!(
            <Sharing as rkyv::ser::Sharing<rkyv::rancor::Error>>::start_sharing(&mut sharing, 0),
            SharingState::Finished(17)
        ));
        assert!(!ctx.rejected.load(Ordering::Relaxed));
        assert!(matches!(
            <Sharing as rkyv::ser::Sharing<rkyv::rancor::Error>>::start_sharing(
                &mut sharing,
                MAX_TYPED_SHARED_POINTERS
            ),
            SharingState::Pending
        ));
        assert_eq!(sharing.count, MAX_TYPED_SHARED_POINTERS);
        assert!(ctx.rejected.load(Ordering::Relaxed));
        ctx.prepare();
        assert_eq!(ctx.sharing.as_ptr(), pointer);
        assert!(!ctx.rejected.load(Ordering::Relaxed));
        // Epoch wrap cannot revive an identity from an old serialization.
        ctx.sharing_epoch = usize::MAX;
        ctx.prepare();
        assert_eq!(ctx.sharing_epoch, 1);
        assert!(ctx.sharing.iter().all(|entry| entry.epoch == 0));
    }

    #[test]
    fn sharing_collision_budget_rejects_without_losing_admitted_aliases() {
        use rkyv::ser::sharing::SharingState;
        type Sharing<'a> = BoundedArchiveSharing<'a>;
        let mut ctx = SerializerCtx::new();
        ctx.prepare();
        let mut sharing = BoundedArchiveSharing {
            entries: &mut ctx.sharing,
            epoch: ctx.sharing_epoch,
            count: 0,
            rejected: &ctx.rejected,
        };
        // All addresses map to bucket zero even after pointer-alignment mixing.
        let stride = TYPED_SHARING_SLOTS * 8;
        for index in 0..MAX_TYPED_SHARING_PROBES {
            let address = index * stride;
            assert!(matches!(
                <Sharing as rkyv::ser::Sharing<rkyv::rancor::Error>>::start_sharing(
                    &mut sharing,
                    address
                ),
                SharingState::Started
            ));
            <Sharing as rkyv::ser::Sharing<rkyv::rancor::Error>>::finish_sharing(
                &mut sharing,
                address,
                index,
            )
            .unwrap();
        }
        assert!(!ctx.rejected.load(Ordering::Relaxed));
        assert!(matches!(
            <Sharing as rkyv::ser::Sharing<rkyv::rancor::Error>>::start_sharing(
                &mut sharing,
                MAX_TYPED_SHARING_PROBES * stride
            ),
            SharingState::Pending
        ));
        assert_eq!(sharing.count, MAX_TYPED_SHARING_PROBES);
        assert!(ctx.rejected.load(Ordering::Relaxed));
        for index in 0..MAX_TYPED_SHARING_PROBES {
            assert!(matches!(
                <Sharing as rkyv::ser::Sharing<rkyv::rancor::Error>>::start_sharing(
                    &mut sharing,
                    index * stride
                ),
                SharingState::Finished(position) if position == index
            ));
        }
    }

    #[test]
    fn shared_archive_aliases_reuse_positions_and_exhaustion_is_typed() {
        #[derive(rkyv::Archive, rkyv::Serialize)]
        struct SharedArchive {
            values: Vec<Arc<u64>>,
        }
        wire_type!(SharedArchive, "typed::bounded::SharedArchive");
        let shared = Arc::new(42);
        let aliases = SharedArchive {
            values: vec![shared; MAX_TYPED_SHARED_POINTERS + 1],
        };
        let mut ctx = SerializerCtx::new();
        encode_typed_in(&aliases, &mut ctx).unwrap();
        let archived =
            rkyv::access::<rkyv::Archived<SharedArchive>, rkyv::rancor::Error>(&ctx.writer)
                .unwrap();
        assert!(std::ptr::eq(
            &*archived.values[0],
            &*archived.values[MAX_TYPED_SHARED_POINTERS]
        ));
        let distinct = SharedArchive {
            values: (0..=MAX_TYPED_SHARED_POINTERS)
                .map(|value| Arc::new(value as u64))
                .collect(),
        };
        assert!(matches!(
            encode_typed_in(&distinct, &mut ctx),
            Err(GossipError::WriteQueueFull)
        ));
        assert!(encode_typed_in(&aliases, &mut ctx).is_ok());
    }

    #[test]
    fn warmed_shared_archives_allocate_no_sharing_storage() {
        #[derive(rkyv::Archive, rkyv::Serialize)]
        struct SharedArchive {
            values: Vec<Arc<u64>>,
        }
        wire_type!(SharedArchive, "typed::bounded::SharingAllocationEvidence");
        let shared = Arc::new(42);
        let value = SharedArchive {
            values: vec![shared; 128],
        };
        let mut ctx = SerializerCtx::new();
        encode_typed_in(&value, &mut ctx).unwrap();
        let pointer = ctx.sharing.as_ptr();
        let (result, counts) = crate::ask_forwarder::allocation_evidence::measure(|| {
            for _ in 0..32 {
                encode_typed_in(&value, &mut ctx)?;
            }
            Ok::<(), GossipError>(())
        });
        result.unwrap();
        assert_eq!(counts.allocations, 0);
        assert_eq!(counts.reallocations, 0);
        assert_eq!(counts.requested_bytes, 0);
        assert_eq!(ctx.sharing.as_ptr(), pointer);
    }

    #[test]
    fn vector_clock_snapshot_allocation_is_bounded_and_explicit() {
        #[derive(rkyv::Archive, rkyv::Serialize)]
        struct ClockArchive {
            clock: crate::VectorClock,
        }
        wire_type!(ClockArchive, "typed::bounded::ClockSnapshotEvidence");
        let node = crate::KeyPair::new_for_testing("clock-snapshot-evidence")
            .peer_id()
            .to_node_id();
        let value = ClockArchive {
            clock: crate::VectorClock::with_node(node),
        };
        let mut ctx = SerializerCtx::new();
        encode_typed_in(&value, &mut ctx).unwrap();
        let writer_pointer = ctx.writer.as_ptr();
        let scratch_pointer = ctx.arena.as_ptr();
        let sharing_pointer = ctx.sharing.as_ptr();
        let (result, counts) = crate::ask_forwarder::allocation_evidence::measure(|| {
            encode_typed_in(&value, &mut ctx)
        });
        result.unwrap();
        assert_eq!(counts.allocations, 0);
        assert_eq!(counts.reallocations, 0);
        assert_eq!(counts.requested_bytes, 0);
        assert_eq!(ctx.writer.as_ptr(), writer_pointer);
        assert_eq!(ctx.arena.as_ptr(), scratch_pointer);
        assert_eq!(ctx.sharing.as_ptr(), sharing_pointer);
        let archived =
            rkyv::access::<rkyv::Archived<ClockArchive>, rkyv::rancor::Error>(&ctx.writer).unwrap();
        assert_eq!(archived.clock.clocks.len(), 1);
        assert_eq!(archived.clock.clocks[0].0.inner, node.inner);

        // Positive control: the compatibility conversion still performs the
        // former one-entry snapshot allocation, and the counter must see it.
        let (snapshot, counts) = crate::ask_forwarder::allocation_evidence::measure(|| {
            std::hint::black_box(crate::VectorClockData::from(&value.clock))
        });
        assert_eq!(snapshot.clocks.len(), 1);
        assert_eq!(counts.allocations, 1);
        assert_eq!(counts.reallocations, 0);
        assert_eq!(
            counts.requested_bytes,
            std::mem::size_of::<(crate::GossipNodeId, u64)>()
        );
    }

    #[test]
    fn warmed_vector_clock_batch_has_no_snapshot_allocation_and_keeps_aliases() {
        #[derive(rkyv::Archive, rkyv::Serialize)]
        struct ClockBatch {
            clocks: Vec<crate::VectorClock>,
            aliases: [Arc<u64>; 2],
        }
        wire_type!(ClockBatch, "typed::bounded::DirectClockBatch");
        let clocks = [0, 1, crate::MAX_INGRESS_VECTOR_CLOCK_ENTRIES]
            .into_iter()
            .map(|count| {
                let clocks = (0..count)
                    .map(|index| {
                        let mut inner = [0; 32];
                        inner[..8].copy_from_slice(&(index as u64).to_le_bytes());
                        (crate::PublicKey { inner }, index as u64)
                    })
                    .collect();
                crate::VectorClock::from(crate::VectorClockData { clocks })
            })
            .collect();
        let shared = Arc::new(73);
        let value = ClockBatch {
            clocks,
            aliases: [Arc::clone(&shared), shared],
        };
        let mut ctx = SerializerCtx::new();
        encode_typed_in(&value, &mut ctx).unwrap();
        let pointers = (
            ctx.writer.as_ptr(),
            ctx.arena.as_ptr(),
            ctx.sharing.as_ptr(),
        );
        let (result, counts) = crate::ask_forwarder::allocation_evidence::measure(|| {
            for _ in 0..32 {
                encode_typed_in(&value, &mut ctx)?;
            }
            Ok::<(), GossipError>(())
        });
        result.unwrap();
        assert_eq!(counts.allocations, 0);
        assert_eq!(counts.reallocations, 0);
        assert_eq!(counts.requested_bytes, 0);
        assert_eq!(ctx.writer.as_ptr(), pointers.0);
        assert_eq!(ctx.arena.as_ptr(), pointers.1);
        assert_eq!(ctx.sharing.as_ptr(), pointers.2);
        let archived =
            rkyv::access::<rkyv::Archived<ClockBatch>, rkyv::rancor::Error>(&ctx.writer).unwrap();
        assert_eq!(archived.clocks.len(), 3);
        assert!(archived.clocks[0].clocks.is_empty());
        assert_eq!(archived.clocks[1].clocks.len(), 1);
        assert_eq!(
            archived.clocks[2].clocks.len(),
            crate::MAX_INGRESS_VECTOR_CLOCK_ENTRIES
        );
        assert!(std::ptr::eq(&*archived.aliases[0], &*archived.aliases[1]));
    }

    #[test]
    fn direct_vector_clock_archive_matches_snapshot_wire_bytes() {
        #[derive(rkyv::Archive, rkyv::Serialize)]
        struct DirectClock {
            clock: crate::VectorClock,
        }
        wire_type!(DirectClock, "typed::bounded::DirectClockWireParity");
        #[derive(rkyv::Archive, rkyv::Serialize)]
        struct SnapshotClock {
            clock: crate::VectorClockData,
        }
        let mut ctx = SerializerCtx::new();
        for count in [0, 1, crate::MAX_INGRESS_VECTOR_CLOCK_ENTRIES] {
            let clocks = (0..count)
                .rev()
                .map(|index| {
                    let mut inner = [0; 32];
                    inner[..8].copy_from_slice(&(index as u64).to_le_bytes());
                    (crate::PublicKey { inner }, index as u64 + 1)
                })
                .collect();
            let direct = DirectClock {
                clock: crate::VectorClock::from(crate::VectorClockData { clocks }),
            };
            let snapshot = SnapshotClock {
                clock: crate::VectorClockData::from(&direct.clock),
            };
            let expected = rkyv::to_bytes::<rkyv::rancor::Error>(&snapshot).unwrap();
            encode_typed_in(&direct, &mut ctx).unwrap();
            assert_eq!(ctx.writer.as_slice(), expected.as_slice());
            let archived =
                rkyv::access::<rkyv::Archived<DirectClock>, rkyv::rancor::Error>(&ctx.writer)
                    .unwrap();
            assert_eq!(archived.clock.clocks.len(), count);
            if count != 0 {
                // A changed clock must change the wire bytes: equality above
                // cannot pass because the serializer omitted every entry.
                let node = snapshot.clock.clocks[0].0;
                direct.clock.increment(node);
                encode_typed_in(&direct, &mut ctx).unwrap();
                assert_ne!(ctx.writer.as_slice(), expected.as_slice());
            }
        }
    }

    #[test]
    fn vector_clock_direct_archive_pins_map_during_writer_mutation() {
        struct MutatingWriter<'a> {
            inner: BoundedArchiveWriter<'a>,
            clock: &'a crate::VectorClock,
            original: crate::GossipNodeId,
            successor: crate::GossipNodeId,
            mutated: bool,
        }
        impl rkyv::ser::Positional for MutatingWriter<'_> {
            fn pos(&self) -> usize {
                rkyv::ser::Positional::pos(&self.inner)
            }
        }
        impl<E: rkyv::rancor::Source> rkyv::ser::Writer<E> for MutatingWriter<'_> {
            fn write(&mut self, bytes: &[u8]) -> std::result::Result<(), E> {
                if !self.mutated && !bytes.is_empty() {
                    self.mutated = true;
                    self.clock.increment(self.original);
                    self.clock.increment(self.successor);
                }
                rkyv::ser::Writer::<E>::write(&mut self.inner, bytes)
            }
        }
        let original = crate::PublicKey { inner: [1; 32] };
        let successor = crate::PublicKey { inner: [2; 32] };
        let clock = crate::VectorClock::with_node(original);
        clock.increment(original);
        let before = clock.clocks.load_full();
        let previous = Arc::downgrade(&before);
        drop(before);
        let mut ctx = SerializerCtx::new();
        ctx.prepare();
        {
            let writer = MutatingWriter {
                inner: BoundedArchiveWriter {
                    buffer: &mut ctx.writer,
                    rejected: &ctx.rejected,
                    observed_oversize: &ctx.observed_oversize,
                },
                clock: &clock,
                original,
                successor,
                mutated: false,
            };
            let mut serializer = rkyv::ser::Serializer::new(
                writer,
                BoundedArchiveAllocator::new(&mut ctx.arena, &ctx.rejected),
                BoundedArchiveSharing {
                    entries: &mut ctx.sharing,
                    epoch: ctx.sharing_epoch,
                    count: 0,
                    rejected: &ctx.rejected,
                },
            );
            rkyv::api::serialize_using::<_, rkyv::rancor::Error>(&clock, &mut serializer).unwrap();
        }
        assert_eq!(clock.len(), 2, "writer mutation must actually run");
        assert_eq!(clock.get(&original), 2);
        assert!(previous.upgrade().is_none());
        let decoded =
            rkyv::access::<rkyv::Archived<crate::VectorClock>, rkyv::rancor::Error>(&ctx.writer)
                .unwrap();
        assert_eq!(decoded.clocks.len(), 1);
        let entry = &decoded.clocks[0];
        assert_eq!(entry.0.inner, original.inner);
        assert_ne!(entry.0.inner, successor.inner);
        assert_eq!(entry.1.to_native(), 1);
        assert!(!ctx.rejected.load(Ordering::Relaxed));
    }

    #[test]
    fn swallowed_sharing_failure_cannot_publish_an_archive() {
        struct SwallowSharingFailure;
        impl rkyv::Archive for SwallowSharingFailure {
            type Archived = rkyv::Archived<u64>;
            type Resolver = <u64 as rkyv::Archive>::Resolver;

            fn resolve(&self, resolver: Self::Resolver, out: rkyv::Place<Self::Archived>) {
                rkyv::Archive::resolve(&0u64, resolver, out);
            }
        }
        impl<'a> rkyv::Serialize<BoundedSerializer<'a>> for SwallowSharingFailure {
            fn serialize(
                &self,
                serializer: &mut BoundedSerializer<'a>,
            ) -> std::result::Result<Self::Resolver, rkyv::rancor::Error> {
                for address in 0..=MAX_TYPED_SHARED_POINTERS {
                    let _ = rkyv::ser::Sharing::start_sharing(serializer, address);
                }
                rkyv::Serialize::serialize(&0u64, serializer)
            }
        }
        wire_type!(
            SwallowSharingFailure,
            "typed::bounded::SwallowSharingFailure"
        );
        let mut ctx = SerializerCtx::new();
        assert!(matches!(
            encode_typed_in(&SwallowSharingFailure, &mut ctx),
            Err(GossipError::WriteQueueFull)
        ));
        assert!(encode_typed_in(&TestMsg { value: 9 }, &mut ctx).is_ok());
    }

    #[test]
    fn archive_writer_exact_boundary_rejects_before_growth() {
        let mut buffer = rkyv::util::AlignedVec::with_capacity(32);
        let capacity = buffer.capacity();
        let pointer = buffer.as_ptr();
        let rejected = AtomicBool::new(false);
        let observed_oversize = AtomicUsize::new(0);
        let mut writer = BoundedArchiveWriter {
            buffer: &mut buffer,
            rejected: &rejected,
            observed_oversize: &observed_oversize,
        };
        for _ in 0..capacity {
            rkyv::ser::Writer::<rkyv::rancor::Error>::write(&mut writer, &[7]).unwrap();
        }
        assert!(rkyv::ser::Writer::<rkyv::rancor::Error>::write(&mut writer, &[8]).is_err());
        assert!(rejected.load(Ordering::Relaxed));
        assert_eq!(buffer.len(), capacity);
        assert_eq!(buffer.capacity(), capacity);
        assert_eq!(buffer.as_ptr(), pointer);
    }

    #[test]
    fn writer_distinguishes_capacity_from_proven_wire_oversize_without_allocating() {
        let mut buffer = rkyv::util::AlignedVec::with_capacity(32);
        let capacity = buffer.capacity();
        let rejected = AtomicBool::new(false);
        let observed_oversize = AtomicUsize::new(0);
        let writer = BoundedArchiveWriter {
            buffer: &mut buffer,
            rejected: &rejected,
            observed_oversize: &observed_oversize,
        };
        assert!(
            writer
                .check_write_len::<rkyv::rancor::Error>(capacity)
                .is_ok()
        );
        assert!(
            writer
                .check_write_len::<rkyv::rancor::Error>(capacity + 1)
                .is_err()
        );
        assert_eq!(observed_oversize.load(Ordering::Relaxed), 0);
        assert!(
            writer
                .check_write_len::<rkyv::rancor::Error>(crate::MAX_STREAM_SIZE)
                .is_err()
        );
        assert_eq!(observed_oversize.load(Ordering::Relaxed), 0);
        assert!(
            writer
                .check_write_len::<rkyv::rancor::Error>(crate::MAX_STREAM_SIZE + 1)
                .is_err()
        );
        assert_eq!(
            observed_oversize.load(Ordering::Relaxed),
            crate::MAX_STREAM_SIZE + 1
        );
        // A later small successful write cannot erase either failure receipt.
        assert!(writer.check_write_len::<rkyv::rancor::Error>(1).is_ok());
        assert!(rejected.load(Ordering::Relaxed));
        assert_eq!(
            observed_oversize.load(Ordering::Relaxed),
            crate::MAX_STREAM_SIZE + 1
        );
        assert!(
            writer
                .check_write_len::<rkyv::rancor::Error>(usize::MAX)
                .is_err()
        );
        assert_eq!(observed_oversize.load(Ordering::Relaxed), usize::MAX);
        assert_eq!(buffer.capacity(), capacity);
        assert!(buffer.is_empty());
    }

    #[test]
    fn archive_scratch_is_aligned_disjoint_bounded_and_reusable() {
        use rkyv::ser::Allocator;
        type Scratch<'a> = BoundedArchiveAllocator<'a>;
        let mut slab = [0u8; 256];
        let rejected = AtomicBool::new(false);
        let mut allocator = BoundedArchiveAllocator::new(&mut slab, &rejected);
        let layout = Layout::from_size_align(32, 64).unwrap();
        // SAFETY: nonzero layouts; pop each pointer once with its original
        // layout in reverse allocation order, without retaining references.
        unsafe {
            let first =
                <Scratch as Allocator<rkyv::rancor::Error>>::push_alloc(&mut allocator, layout)
                    .unwrap();
            let second =
                <Scratch as Allocator<rkyv::rancor::Error>>::push_alloc(&mut allocator, layout)
                    .unwrap();
            assert_eq!(first.cast::<u8>().as_ptr() as usize % 64, 0);
            assert_eq!(second.cast::<u8>().as_ptr() as usize % 64, 0);
            assert_ne!(first.cast::<u8>(), second.cast::<u8>());
            first.cast::<u8>().as_ptr().write(17);
            second.cast::<u8>().as_ptr().write(23);
            assert_eq!(first.cast::<u8>().as_ptr().read(), 17);
            let before = allocator.cursor;
            let too_large = Layout::from_size_align(257, 1).unwrap();
            assert!(
                <Scratch as Allocator<rkyv::rancor::Error>>::push_alloc(&mut allocator, too_large)
                    .is_err()
            );
            assert_eq!(allocator.cursor, before);
            assert!(rejected.load(Ordering::Relaxed));
            <Scratch as Allocator<rkyv::rancor::Error>>::pop_alloc(
                &mut allocator,
                second.cast(),
                layout,
            )
            .unwrap();
            let reused =
                <Scratch as Allocator<rkyv::rancor::Error>>::push_alloc(&mut allocator, layout)
                    .unwrap();
            assert_eq!(reused.cast::<u8>(), second.cast::<u8>());
            <Scratch as Allocator<rkyv::rancor::Error>>::pop_alloc(
                &mut allocator,
                reused.cast(),
                layout,
            )
            .unwrap();
            <Scratch as Allocator<rkyv::rancor::Error>>::pop_alloc(
                &mut allocator,
                first.cast(),
                layout,
            )
            .unwrap();
        }
    }

    #[test]
    fn oversized_typed_archive_rejects_and_same_context_recovers() {
        #[derive(rkyv::Archive, rkyv::Serialize)]
        struct LargeArchive {
            bytes: Vec<u8>,
        }
        wire_type!(LargeArchive, "typed::bounded::LargeArchive");
        let value = LargeArchive {
            bytes: vec![0; MAX_TYPED_ARCHIVE_BYTES + 1],
        };
        let mut ctx = SerializerCtx::new();
        assert!(matches!(
            encode_typed_in(&value, &mut ctx),
            Err(GossipError::WriteQueueFull)
        ));
        let writer_pointer = ctx.writer.as_ptr();
        let scratch_pointer = ctx.arena.as_ptr();
        assert_eq!(ctx.writer.capacity(), MAX_TYPED_ARCHIVE_BYTES);
        assert_eq!(ctx.arena.capacity(), MAX_TYPED_SCRATCH_BYTES);
        let length = encode_typed_in(&TestMsg { value: 42 }, &mut ctx).unwrap();
        assert!(length > 0);
        assert!(!ctx.rejected.load(Ordering::Relaxed));
        assert_eq!(ctx.writer.as_ptr(), writer_pointer);
        assert_eq!(ctx.arena.as_ptr(), scratch_pointer);
    }

    #[test]
    fn typed_archive_byte_view_keeps_fixed_reservations_alive() {
        let payload = encode_typed_pooled(&TestMsg { value: 42 }).unwrap();
        assert_eq!(
            payload.retained_bytes(),
            MAX_TYPED_ARCHIVE_BYTES
                + MAX_TYPED_SCRATCH_BYTES
                + TYPED_SHARING_SLOTS * std::mem::size_of::<SharingEntry>()
        );
        let pointer = payload.chunk().as_ptr();
        let bytes = payload.into_remaining_bytes();
        let view = bytes.slice(0..1);
        drop(bytes);
        assert_eq!(view.as_ptr(), pointer);
        // The Bytes owner keeps the serializer context and both reservations;
        // a last-view drop, not serialization completion, returns admission.
        assert_eq!(view.len(), 1);
    }

    #[test]
    fn small_typed_reply_owner_fits_existing_response_lane() {
        let payload = encode_typed_pooled(&TestMsg { value: 7 }).unwrap();
        let (payload, prefix, wire_len) = typed_payload_parts::<TestMsg>(payload);
        let prefix_len = prefix.as_ref().map_or(0, |prefix| prefix.len());
        assert_eq!(wire_len, payload.len() + prefix_len);
        assert!(
            payload.retained_bytes() + prefix_len
                <= crate::connection_pool::RESPONSE_BATCH_BYTE_CAP
        );
        assert!(payload.retained_bytes() <= crate::ask_forwarder::DEFAULT_FORWARD_BYTE_BUDGET);
    }

    /// The type-hash prefix and its verification must be unconditional: a
    /// debug binary and a release binary must speak the exact same wire
    /// format for every typed payload. `cargo test` always runs in the dev
    /// profile (that attribute is true), so a behavioral test cannot observe
    /// the release path directly; this asserts the *source* no longer
    /// branches on it, which is the only way the two build modes can be
    /// guaranteed to agree. The needles are built by concatenation (never
    /// written as a contiguous literal anywhere in this file) so this test
    /// cannot trip over its own source text.
    #[test]
    fn typed_prefix_logic_does_not_vary_by_cfg_debug_assertions() {
        let source = include_str!("typed.rs");
        let positive: String = ["cfg", "(", "debug_assertions", ")"].concat();
        let negative: String = ["cfg", "(", "not", "(", "debug_assertions", ")", ")"].concat();
        assert!(
            !source.contains(&positive) && !source.contains(&negative),
            "typed.rs must not gate the type-hash prefix (encode/decode, typed_payload_parts) \
             on debug_assertions -- debug and release must be wire-compatible for every typed payload"
        );
    }

    #[test]
    fn owned_payload_transfer_preserves_capacity_pointer_and_remaining_view() {
        let mut bytes = Vec::with_capacity(1024);
        bytes.extend_from_slice(b"abc");
        let pointer = bytes.as_ptr();
        let capacity = bytes.capacity();
        let mut payload = PooledPayload::from_owned(crate::ForwardPayload::from(bytes));
        assert_eq!(payload.chunk().as_ptr(), pointer);
        assert_eq!(payload.retained_bytes(), capacity);
        payload.advance(1);
        assert_eq!(payload.chunk(), b"bc");
        let view = payload.into_remaining_bytes();
        assert_eq!(view.as_ptr(), pointer.wrapping_add(1));
        assert_eq!(&view[..], b"bc");
    }

    #[test]
    fn typed_serializer_checkout_exhaustion_returns_backpressure() {
        // Exercise the production admission helper on an isolated pool: global
        // exhaustion would race unrelated typed encodes in parallel unit tests.
        let pool = ArrayQueue::new(SERIALIZER_POOL_SIZE);
        for _ in 0..SERIALIZER_POOL_SIZE {
            assert!(pool.push(Box::new(SerializerCtx::new())).is_ok());
        }
        let mut held = Vec::new();
        for _ in 0..SERIALIZER_POOL_SIZE {
            held.push(acquire_ctx_from(&pool).unwrap());
        }
        assert!(matches!(
            acquire_ctx_from(&pool),
            Err(GossipError::WriteQueueFull)
        ));
        let mut returned = held.pop().unwrap();
        assert!(encode_typed_in(&TestMsg { value: 9 }, &mut returned).is_ok());
        assert!(pool.push(returned).is_ok());
        let mut recovered = acquire_ctx_from(&pool).unwrap();
        assert!(encode_typed_in(&TestMsg { value: 11 }, &mut recovered).is_ok());
        assert!(pool.is_empty());
    }

    #[test]
    fn typed_pool_exhaustion_is_bounded_backpressure() {
        let mut held = Vec::new();
        for _ in 0..CHECKED_OUT_BYTE_BUDGET / MAX_POOLED_BYTE_CAPACITY {
            held.push(PooledPayload::try_from_pooled_bytes(1, |out| out.push(7)).unwrap());
        }
        let before = CHECKED_OUT_BYTES.load(Ordering::Acquire);
        let mut called = false;
        assert!(matches!(
            PooledPayload::try_from_pooled_bytes(1, |_| called = true),
            Err(GossipError::WriteQueueFull)
        ));
        assert!(!called);
        assert_eq!(CHECKED_OUT_BYTES.load(Ordering::Acquire), before);
        held.pop();
        let recovered = PooledPayload::try_from_pooled_bytes(1, |out| out.push(8)).unwrap();
        assert_eq!(recovered.chunk(), &[8]);
    }

    #[test]
    fn bounded_byte_fill_rejects_growth_and_preserves_owner_charge() {
        let before = CHECKED_OUT_BYTES.load(Ordering::Acquire);
        assert!(matches!(
            PooledPayload::try_from_pooled_bytes(1, |out| out.extend_from_slice(b"too long")),
            Err(GossipError::WriteQueueFull)
        ));
        assert_eq!(CHECKED_OUT_BYTES.load(Ordering::Acquire), before);
        let payload =
            PooledPayload::try_from_pooled_bytes(3, |out| out.extend_from_slice(b"abc")).unwrap();
        let bytes = payload.into_remaining_bytes();
        let view = bytes.slice(1..2);
        drop(bytes);
        assert_eq!(
            CHECKED_OUT_BYTES.load(Ordering::Acquire),
            before + MAX_POOLED_BYTE_CAPACITY
        );
        assert_eq!(&view[..], b"b");
        drop(view);
        assert_eq!(CHECKED_OUT_BYTES.load(Ordering::Acquire), before);
    }

    #[test]
    fn pooled_payload_buf_semantics() {
        let msg = TestMsg { value: 42 };
        let mut payload = encode_typed_pooled(&msg).unwrap();
        let remaining = payload.remaining();
        assert!(remaining > 0);
        assert_eq!(payload.chunk().len(), remaining);

        let advance_by = 1.min(remaining);
        payload.advance(advance_by);
        assert_eq!(payload.remaining(), remaining - advance_by);
    }

    #[test]
    #[should_panic(expected = "cannot advance past remaining bytes")]
    fn pooled_payload_rejects_advance_past_remaining() {
        let mut payload = PooledPayload::try_from_pooled_bytes(3, |out| {
            out.extend_from_slice(b"abc");
        })
        .expect("small pooled payload");

        payload.advance(4);
    }

    #[test]
    fn pool_reuse_and_cap_behavior() {
        let msg = TestMsg { value: 7 };
        let payload = encode_typed_pooled(&msg).unwrap();
        drop(payload);

        #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
        struct BigMsg {
            data: Vec<u8>,
        }
        wire_type!(BigMsg, "typed::BigMsg");

        let big = BigMsg {
            data: vec![0u8; MAX_POOLED_BYTE_CAPACITY + 1024],
        };
        let big_payload = encode_typed_pooled(&big).unwrap();
        assert!(big_payload.len() > MAX_POOLED_BYTE_CAPACITY);
        assert_eq!(
            big_payload.retained_bytes(),
            MAX_TYPED_ARCHIVE_BYTES
                + MAX_TYPED_SCRATCH_BYTES
                + TYPED_SHARING_SLOTS * std::mem::size_of::<SharingEntry>()
        );
        drop(big_payload);
    }

    #[test]
    fn pooled_byte_payload_allocates_bounded_buffer_when_warm_pool_too_small() {
        let payload = PooledPayload::try_from_pooled_bytes(65_507, |out| {
            out.extend_from_slice(&vec![0u8; 65_507]);
        })
        .expect("bounded on-demand byte payload");
        assert_eq!(payload.len(), 65_507);

        // Above the retention bound the encode still succeeds — on an
        // unpooled buffer. The bound caps what the pool *keeps*, never what
        // callers may send.
        assert!(PooledPayload::try_from_pooled_bytes(MAX_POOLED_BYTE_CAPACITY + 1, |_| {}).is_ok());
    }

    #[test]
    fn typed_payload_parts_includes_hash_unconditionally() {
        let msg = TestMsg { value: 1 };
        let payload = encode_typed_pooled(&msg).unwrap();
        let (_payload, prefix, total_len) = typed_payload_parts::<TestMsg>(payload);

        assert!(total_len >= 16);
        assert!(prefix.is_some());
    }

    #[test]
    fn archived_body_is_validated_alongside_the_prefix() {
        #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, PartialEq)]
        struct AlignedTest {
            v: u128,
        }
        wire_type!(AlignedTest, "typed::AlignedTest");

        // Encode (prefix included), then copy into an aligned receive buffer to
        // simulate the real receive path.
        let pooled = encode_typed_pooled(&AlignedTest { v: 7 }).unwrap();
        let (payload, prefix, total_len) = typed_payload_parts::<AlignedTest>(pooled);
        let mut encoded = Vec::with_capacity(total_len);
        if let Some(prefix) = prefix {
            encoded.extend_from_slice(&prefix);
        }
        encoded.extend_from_slice(payload.chunk());
        let pool = Arc::new(AlignedBytesPool::new(1));
        let aligned = AlignedBytes::from_pooled_slice(&encoded, Arc::clone(&pool)).unwrap();
        let bytes: Bytes = aligned.into();

        let archived = decode_typed_archived::<AlignedTest>(bytes).unwrap();
        let a = archived.archived().unwrap();
        assert_eq!(a.v, 7);

        let mut malformed = encoded;
        malformed.pop();
        assert!(
            decode_typed_archived::<AlignedTest>(Bytes::from(malformed)).is_err(),
            "a matching type hash must not bypass archived byte validation"
        );
    }

    fn native_test_wire(value: u64) -> Vec<u8> {
        let payload = encode_typed_pooled(&TestMsg { value }).unwrap();
        let (payload, prefix, len) = typed_payload_parts::<TestMsg>(payload);
        let mut encoded = Vec::with_capacity(len);
        encoded.extend_from_slice(&prefix.unwrap());
        encoded.extend_from_slice(payload.as_ref());
        encoded
    }

    #[test]
    fn native_archived_response_retains_pointer_capacity_and_pool_checkout() {
        use crate::ask_forwarder::allocation_evidence;

        let encoded = native_test_wire(23);
        let mut storage = crate::aligned::AlignedBuffer::with_capacity(4096);
        storage.resize(16, 0);
        storage.extend_from_slice(&encoded);
        let pointer = storage.as_ptr().wrapping_add(16);
        let capacity = storage.capacity();
        let end = storage.len();
        let native = AlignedBytes::from_aligned_vec(storage)
            .into_range(16..end)
            .unwrap();
        let (archived, counts) = allocation_evidence::measure(|| {
            let archived = decode_typed_archived_owned::<TestMsg>(native).unwrap();
            assert_eq!(archived.archived().unwrap().value, 23);
            archived
        });
        assert_eq!(counts, allocation_evidence::Counts::default());
        assert_eq!(archived.as_bytes().as_ptr(), pointer.wrapping_add(16));
        assert_eq!(archived.retained_capacity(), Some(capacity));
        assert!(capacity > encoded.len());
        let (shared, counts) = allocation_evidence::measure(|| archived.into_bytes());
        assert!(
            counts.allocations > 0,
            "explicit sharing is the positive control"
        );
        assert_eq!(shared.as_ptr(), pointer);
        assert_eq!(shared.as_ref(), encoded.as_slice());
        let compatible = decode_typed_archived::<TestMsg>(shared).unwrap();
        assert_eq!(compatible.archived().unwrap().value, 23);
        assert_eq!(compatible.retained_capacity(), None);

        let pool = Arc::new(AlignedBytesPool::new(1));
        let native = AlignedBytes::from_pooled_slice(&encoded, pool.clone()).unwrap();
        let pointer = native.as_ref().as_ptr();
        let capacity = native.retained_capacity();
        let owners = Arc::strong_count(&pool);
        let (archived, counts) = allocation_evidence::measure(|| {
            decode_typed_archived_owned::<TestMsg>(native).unwrap()
        });
        assert_eq!(counts, allocation_evidence::Counts::default());
        assert_eq!(Arc::strong_count(&pool), owners);
        assert_eq!(archived.as_bytes().as_ptr(), pointer.wrapping_add(16));
        assert_eq!(archived.retained_capacity(), capacity);
        assert!(pool.get_buffer(1).is_err());
        drop(archived);
        assert!(
            pool.get_buffer(1).is_ok(),
            "drop returns the exact checkout"
        );
    }

    #[test]
    fn native_archived_response_rejects_bad_hash_and_body_and_returns_owner() {
        let encoded = native_test_wire(31);
        let pool = Arc::new(AlignedBytesPool::new(1));
        for invalid in [
            Vec::new(),
            encoded[..15].to_vec(),
            {
                let mut bad_hash = encoded.to_vec();
                bad_hash[0] ^= 1;
                bad_hash
            },
            encoded[..16].to_vec(),
            encoded[..encoded.len() - 1].to_vec(),
        ] {
            let native = AlignedBytes::from_pooled_slice(&invalid, pool.clone()).unwrap();
            assert!(decode_typed_archived_owned::<TestMsg>(native).is_err());
            assert!(pool.get_buffer(1).is_ok(), "rejection returns the checkout");
        }
        let valid = AlignedBytes::from_pooled_slice(&encoded, pool).unwrap();
        assert_eq!(
            decode_typed_archived_owned::<TestMsg>(valid)
                .unwrap()
                .archived()
                .unwrap()
                .value,
            31
        );
    }

    #[test]
    fn archived_decode_rejects_misaligned_caller_bytes() {
        let payload = encode_typed_pooled(&TestMsg { value: 9 }).unwrap();
        let (payload, prefix, total_len) = typed_payload_parts::<TestMsg>(payload);
        let mut encoded = Vec::with_capacity(total_len);
        if let Some(prefix) = prefix {
            encoded.extend_from_slice(&prefix);
        }
        encoded.extend_from_slice(payload.chunk());
        let archive_alignment = std::mem::align_of::<<TestMsg as rkyv::Archive>::Archived>();
        assert!(archive_alignment > 1);
        let body_offset = 16;
        let mut storage = vec![0_u8; encoded.len() + archive_alignment];
        let storage_address = storage.as_ptr() as usize;
        let offset = (0..archive_alignment)
            .find(|offset| (storage_address + offset + body_offset) % archive_alignment != 0)
            .expect("an over-allocation must contain a misaligned offset");
        storage[offset..offset + encoded.len()].copy_from_slice(&encoded);
        let misaligned = Bytes::from(storage).slice(offset..offset + encoded.len());

        let Err(error) = decode_typed_archived::<TestMsg>(misaligned) else {
            panic!("misaligned caller bytes must be rejected");
        };
        assert!(matches!(
            error,
            GossipError::InvalidConfig(message)
                if message.contains("typed archive alignment")
        ));
    }

    #[test]
    fn archive_alignment_cannot_exceed_transport_guarantee() {
        #[repr(align(32))]
        struct OverAligned;

        let error = validate_archive_alignment::<OverAligned>(&[], "OverAligned")
            .expect_err("over-aligned archives must fail closed");
        assert!(matches!(
            error,
            GossipError::InvalidConfig(message)
                if message.contains("exceeding the 16-byte payload guarantee")
        ));
    }
}

enum ArchivedPayload {
    Native(crate::AlignedBytes),
    Shared(Bytes),
}

impl AsRef<[u8]> for ArchivedPayload {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Native(bytes) => bytes.as_ref(),
            Self::Shared(bytes) => bytes.as_ref(),
        }
    }
}

/// Zero-copy archived view retaining its complete original receive owner.
/// Native responses do not promote their owner to shared `Bytes` metadata.
pub struct ArchivedBytes<T> {
    bytes: ArchivedPayload,
    offset: usize,
    _marker: PhantomData<T>,
}

impl<T> ArchivedBytes<T> {
    /// Access the archive body without the unconditional type hash prefix.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes.as_ref()[self.offset..]
    }

    /// Complete retained allocation capacity, when supplied by a native owner.
    /// Arbitrary compatibility `Bytes` cannot prove their retained capacity.
    pub fn retained_capacity(&self) -> Option<usize> {
        match &self.bytes {
            ArchivedPayload::Native(bytes) => bytes.retained_capacity(),
            ArchivedPayload::Shared(_) => None,
        }
    }

    /// Explicit compatibility promotion, including the type hash prefix.
    /// Native owners may allocate shared-owner metadata at this boundary.
    pub fn into_bytes(self) -> Bytes {
        match self.bytes {
            ArchivedPayload::Native(bytes) => bytes.into_bytes(),
            ArchivedPayload::Shared(bytes) => bytes,
        }
    }
}

impl<T> ArchivedBytes<T>
where
    T: WireType + rkyv::Archive,
    for<'a> T::Archived: rkyv::Portable
        + rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    /// Access the archived payload with validation.
    pub fn archived(&self) -> Result<&<T as rkyv::Archive>::Archived> {
        Ok(rkyv::access::<
            <T as rkyv::Archive>::Archived,
            rkyv::rancor::Error,
        >(self.as_bytes())?)
    }
}

/// Decode a typed message from the wire.
///
/// Verifies and strips the type hash prefix. Unconditional in every build
/// mode -- see `encode_typed`.
pub fn decode_typed<T>(payload: &[u8]) -> Result<T>
where
    T: WireType + rkyv::Archive,
    for<'a> T::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        > + rkyv::Deserialize<T, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
{
    const PREFIX_LEN: usize = 16;
    if payload.len() < PREFIX_LEN {
        return Err(GossipError::InvalidConfig(format!(
            "typed payload too short for type hash ({})",
            T::TYPE_NAME
        )));
    }
    let mut hash_bytes = [0u8; 8];
    hash_bytes.copy_from_slice(&payload[..8]);
    let hash = u64::from_be_bytes(hash_bytes);
    if hash != T::TYPE_HASH {
        return Err(GossipError::InvalidConfig(format!(
            "typed payload hash mismatch for {}: expected {:016x}, got {:016x}",
            T::TYPE_NAME,
            T::TYPE_HASH,
            hash
        )));
    }
    let body = &payload[PREFIX_LEN..];
    let archived = rkyv::access::<T::Archived, rkyv::rancor::Error>(body)?;
    let mut pool = rkyv::de::Pool::new();
    let mut deserializer = rkyv::rancor::Strategy::wrap(&mut pool);
    Ok(rkyv::Deserialize::deserialize(archived, &mut deserializer)?)
}

/// Decode a typed message into an archived view (zero-copy).
///
/// Verifies and strips the type hash prefix without copying. Unconditional
/// in every build mode -- see `encode_typed`.
pub fn decode_typed_archived<T>(payload: Bytes) -> Result<ArchivedBytes<T>>
where
    T: WireType + rkyv::Archive,
    for<'a> T::Archived: rkyv::Portable
        + rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    decode_archived_payload(ArchivedPayload::Shared(payload))
}

/// Decode a typed archive while retaining its native aligned receive owner.
/// Hash, alignment and archive validation apply in every build mode. Moving
/// the owner adds no shared-owner metadata allocation or reference count.
pub fn decode_typed_archived_owned<T>(payload: crate::AlignedBytes) -> Result<ArchivedBytes<T>>
where
    T: WireType + rkyv::Archive,
    for<'a> T::Archived: rkyv::Portable
        + rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    decode_archived_payload(ArchivedPayload::Native(payload))
}

fn decode_archived_payload<T>(bytes: ArchivedPayload) -> Result<ArchivedBytes<T>>
where
    T: WireType + rkyv::Archive,
    for<'a> T::Archived: rkyv::Portable
        + rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    const PREFIX_LEN: usize = 16;
    let payload = bytes.as_ref();
    if payload.len() < PREFIX_LEN {
        return Err(GossipError::InvalidConfig(format!(
            "typed payload too short for type hash ({})",
            T::TYPE_NAME
        )));
    }
    let mut hash_bytes = [0u8; 8];
    hash_bytes.copy_from_slice(&payload[..8]);
    let hash = u64::from_be_bytes(hash_bytes);
    if hash != T::TYPE_HASH {
        return Err(GossipError::InvalidConfig(format!(
            "typed payload hash mismatch for {}: expected {:016x}, got {:016x}",
            T::TYPE_NAME,
            T::TYPE_HASH,
            hash
        )));
    }
    validate_archive_alignment::<T::Archived>(&payload[PREFIX_LEN..], T::TYPE_NAME)?;
    // Validate before exposing the zero-copy wrapper. The wrapper owns the
    // bytes, so the validated archive remains stable for its lifetime.
    rkyv::access::<T::Archived, rkyv::rancor::Error>(&payload[PREFIX_LEN..])?;
    Ok(ArchivedBytes {
        bytes,
        offset: PREFIX_LEN,
        _marker: PhantomData,
    })
}

/// Implement WireType with a stable, shared string identifier.
#[macro_export]
macro_rules! wire_type {
    ($ty:ty, $name:expr) => {
        impl $crate::typed::WireType for $ty {
            const TYPE_HASH: u64 = $crate::typed::fnv1a_hash($name);
            const TYPE_NAME: &'static str = $name;
        }
    };
}
