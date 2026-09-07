use crossbeam_queue::ArrayQueue;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use rkyv::util::AlignedVec;

use crate::GossipError;

pub const PAYLOAD_ALIGNMENT: usize = 16;
pub const DEFAULT_ALIGNED_POOL_SIZE: usize = 64;
pub const MAX_POOLED_ALIGNED_CAPACITY: usize = 8 * 1024 * 1024; // 8MB
pub const DEFAULT_ALIGNED_BUFFER_CAPACITY: usize = 256;
/// Idle retained-capacity budget for the aligned receive pool, shared by every
/// connection on a registry. Caps high-water reuse after large-frame bursts.
pub const DEFAULT_ALIGNED_POOL_BYTE_BUDGET: usize = 64 * 1024 * 1024;

pub type AlignedBuffer = AlignedVec<PAYLOAD_ALIGNMENT>;

/// Pooled aligned buffer owner for Bytes::from_owner.
#[derive(Debug)]
pub struct PooledAlignedBuffer {
    buffer: AlignedBuffer,
    pool: Arc<AlignedBytesPool>,
}

impl PooledAlignedBuffer {
    pub fn with_len(len: usize, pool: Arc<AlignedBytesPool>) -> Self {
        // This is used by framed receive paths which take `&mut [u8]` slices.
        // For safety, the slice must point to initialized memory, so we resize (zero-fill).
        // We still reuse the pooled allocation to avoid per-message heap allocs.
        let mut buffer = pool.get_buffer(len);
        if buffer.len() != len {
            buffer.resize(len, 0);
        }
        Self { buffer, pool }
    }

    /// Create a pooled buffer with logical length set but without zero-filling the contents.
    ///
    /// # Safety
    ///
    /// Callers must fully initialize every byte before any read from the slice.
    pub unsafe fn with_len_uninit(len: usize, pool: Arc<AlignedBytesPool>) -> Self {
        let mut buffer = pool.get_buffer(len);
        if buffer.capacity() < len {
            buffer.reserve(len - buffer.len());
        }
        unsafe {
            buffer.set_len(len);
        }
        Self { buffer, pool }
    }

    pub fn from_slice(data: &[u8], pool: Arc<AlignedBytesPool>) -> Self {
        let mut buffer = pool.get_buffer(data.len());
        buffer.extend_from_slice(data);
        Self { buffer, pool }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buffer.as_mut_slice()
    }

    pub fn truncate(&mut self, len: usize) {
        let new_len = len.min(self.buffer.len());
        if self.buffer.len() > new_len {
            self.buffer.resize(new_len, 0);
        }
    }

    pub fn into_aligned_bytes(self) -> AlignedBytes {
        AlignedBytes::from_pooled_buffer(self)
    }
}

impl AsRef<[u8]> for PooledAlignedBuffer {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref()
    }
}

impl Drop for PooledAlignedBuffer {
    fn drop(&mut self) {
        let buffer = std::mem::take(&mut self.buffer);
        self.pool.return_buffer(buffer);
    }
}

#[derive(Debug)]
enum AlignedBytesInner {
    Bytes(Bytes),
    Pooled(PooledAlignedBuffer),
}

#[derive(Debug)]
pub struct AlignedBytes {
    inner: AlignedBytesInner,
    offset: usize,
    len: usize,
}

impl AlignedBytes {
    pub fn from_bytes(bytes: Bytes) -> Result<Self, GossipError> {
        // CRITICAL_PATH: enforce alignment for zero-copy archived access.
        if (bytes.as_ptr() as usize) % PAYLOAD_ALIGNMENT != 0 {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "misaligned payload buffer",
            )));
        }
        let len = bytes.len();
        Ok(Self {
            inner: AlignedBytesInner::Bytes(bytes),
            offset: 0,
            len,
        })
    }

    pub fn from_aligned_vec(vec: AlignedBuffer) -> Self {
        let bytes = Bytes::from_owner(vec);
        // AlignedVec guarantees alignment; treat violation as a bug.
        Self::from_bytes(bytes).expect("aligned buffer must be aligned")
    }

    pub fn from_pooled_buffer(buffer: PooledAlignedBuffer) -> Self {
        let len = buffer.as_ref().len();
        Self::from_pooled_buffer_range(buffer, 0, len)
            .expect("aligned pooled buffer must be aligned")
    }

    pub fn from_pooled_buffer_range(
        buffer: PooledAlignedBuffer,
        offset: usize,
        len: usize,
    ) -> Result<Self, GossipError> {
        let buf_len = buffer.as_ref().len();
        let end = offset.saturating_add(len);
        if end > buf_len {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "payload range out of bounds",
            )));
        }
        // R5: an empty view has no bytes to dereference, so there is nothing
        // to align. When the shared pool is exhausted, `get_buffer` falls
        // back to `AlignedBuffer::with_capacity(0)`, and rkyv's `AlignedVec`
        // represents that as `NonNull::dangling()` (address == align_of::<u8>()
        // == 1), NOT the declared `PAYLOAD_ALIGNMENT`. That is expected,
        // benign behavior for a zero-length allocation, not a corrupted
        // buffer, so skip the pointer-alignment check entirely for `len ==
        // 0` rather than dereferencing (or even forming a pointer to) a
        // buffer we will never read. The `len > 0` path below is untouched:
        // the alignment invariant still holds for every real payload.
        if len == 0 {
            return Ok(Self {
                inner: AlignedBytesInner::Pooled(buffer),
                offset,
                len: 0,
            });
        }
        let ptr = unsafe { buffer.as_ref().as_ptr().add(offset) };
        if (ptr as usize) % PAYLOAD_ALIGNMENT != 0 {
            return Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "misaligned payload buffer",
            )));
        }
        Ok(Self {
            inner: AlignedBytesInner::Pooled(buffer),
            offset,
            len,
        })
    }

    pub fn from_pooled_slice(data: &[u8], pool: Arc<AlignedBytesPool>) -> Self {
        let buffer = PooledAlignedBuffer::from_slice(data, pool);
        Self::from_pooled_buffer(buffer)
    }

    pub fn into_bytes(self) -> Bytes {
        match self.inner {
            AlignedBytesInner::Bytes(bytes) => bytes,
            AlignedBytesInner::Pooled(buffer) => {
                let bytes = Bytes::from_owner(buffer);
                bytes.slice(self.offset..self.offset + self.len)
            }
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn slice(&self, range: std::ops::Range<usize>) -> Result<Self, GossipError> {
        match &self.inner {
            AlignedBytesInner::Bytes(bytes) => Self::from_bytes(bytes.slice(range)),
            AlignedBytesInner::Pooled(_) => Err(GossipError::Network(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "slice on pooled aligned bytes is not supported",
            ))),
        }
    }
}

impl AsRef<[u8]> for AlignedBytes {
    fn as_ref(&self) -> &[u8] {
        match &self.inner {
            AlignedBytesInner::Bytes(bytes) => bytes.as_ref(),
            AlignedBytesInner::Pooled(buffer) => {
                &buffer.as_ref()[self.offset..self.offset + self.len]
            }
        }
    }
}

impl Deref for AlignedBytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl From<AlignedBytes> for Bytes {
    fn from(value: AlignedBytes) -> Self {
        value.into_bytes()
    }
}

/// Pool for aligned buffers used in receive paths.
#[derive(Debug)]
pub struct AlignedBytesPool {
    queue: ArrayQueue<AlignedBuffer>,
    retained_bytes: AtomicUsize,
    budget: usize,
}

impl AlignedBytesPool {
    pub fn new(pool_size: usize) -> Self {
        Self::new_with_budget(pool_size, DEFAULT_ALIGNED_POOL_BYTE_BUDGET)
    }

    pub fn new_with_budget(pool_size: usize, budget: usize) -> Self {
        let queue = ArrayQueue::new(pool_size.max(1));
        let pool = Self {
            queue,
            retained_bytes: AtomicUsize::new(0),
            budget,
        };
        for _ in 0..pool_size {
            let buffer = AlignedBuffer::with_capacity(DEFAULT_ALIGNED_BUFFER_CAPACITY);
            let cap = buffer.capacity();
            if !pool.try_reserve(cap) {
                break;
            }
            if pool.queue.push(buffer).is_err() {
                pool.release_reservation(cap);
                break;
            }
        }
        pool
    }

    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes.load(Ordering::Acquire)
    }

    fn try_reserve(&self, bytes: usize) -> bool {
        let mut current = self.retained_bytes.load(Ordering::Relaxed);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                return false;
            };
            if next > self.budget {
                return false;
            }
            match self.retained_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    fn release_reservation(&self, bytes: usize) {
        let mut current = self.retained_bytes.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_sub(bytes);
            match self.retained_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    /// CRITICAL_PATH: acquire aligned buffer without extra allocations.
    pub fn get_buffer(&self, min_capacity: usize) -> AlignedBuffer {
        if let Some(mut buffer) = self.queue.pop() {
            self.release_reservation(buffer.capacity());
            if buffer.capacity() < min_capacity {
                buffer.reserve(min_capacity - buffer.len());
            }
            buffer.clear();
            buffer
        } else {
            let mut buffer = AlignedBuffer::with_capacity(min_capacity);
            buffer.clear();
            buffer
        }
    }

    /// CRITICAL_PATH: return aligned buffer to pool.
    pub fn return_buffer(&self, mut buffer: AlignedBuffer) {
        buffer.clear();
        let cap = buffer.capacity();
        if cap > MAX_POOLED_ALIGNED_CAPACITY {
            return;
        }
        if !self.try_reserve(cap) {
            return;
        }
        if self.queue.push(buffer).is_err() {
            self.release_reservation(cap);
        }
    }

    pub fn available_count(&self) -> usize {
        self.queue.len()
    }
}

impl Default for AlignedBytesPool {
    fn default() -> Self {
        Self::new(DEFAULT_ALIGNED_POOL_SIZE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aligned_bytes_pool_reuses_buffers_and_alignment() {
        let pool = Arc::new(AlignedBytesPool::new(2));
        assert_eq!(pool.available_count(), 2);

        let bytes = AlignedBytes::from_pooled_slice(&[1u8, 2, 3, 4], pool.clone());
        let ptr = bytes.as_ref().as_ptr() as usize;
        assert_eq!(ptr % PAYLOAD_ALIGNMENT, 0);
        drop(bytes);

        assert_eq!(pool.available_count(), 2);
    }

    // R5 RED: with an exhausted pool (empty queue, the state every connection
    // reaches once ~64+ buffers are checked out under load), `get_buffer`
    // falls back to `AlignedBuffer::with_capacity(min_capacity)`. For
    // `min_capacity == 0` (a legitimate zero-length stream — see
    // `protocol::tests::finalize_empty_stream_is_allowed`), rkyv's
    // `AlignedVec` stores a `NonNull::dangling()` pointer whose address is
    // `align_of::<u8>() == 1`, not the declared 16-byte `PAYLOAD_ALIGNMENT`.
    // `from_pooled_buffer` must not treat that as a misaligned-buffer bug: an
    // empty view has nothing to dereference, so it must hand back a canonical
    // empty `AlignedBytes` instead of panicking on the `.expect(..)` alignment
    // invariant meant for real (non-empty) payloads.
    #[test]
    fn empty_pool_zero_len_pooled_buffer_does_not_panic() {
        // ArrayQueue requires non-zero capacity, so model "pool exhausted
        // under load" the same way production traffic reaches it: check the
        // single slot out and hold onto it.
        let pool = Arc::new(AlignedBytesPool::new(1));
        let _checked_out = pool.get_buffer(64);
        assert_eq!(pool.available_count(), 0, "pool must be exhausted");

        let pooled = PooledAlignedBuffer::with_len(0, pool);
        let bytes = AlignedBytes::from_pooled_buffer(pooled);
        assert_eq!(bytes.len(), 0);
        assert!(bytes.is_empty());
        assert_eq!(bytes.as_ref(), b"");
    }

    // R5 HARDEN: exhaustively walk every (pool-state x len x offset)
    // combination that can actually arise in production instead of a single
    // regression case. No proptest/quickcheck dependency is warranted here —
    // the reachable state space is small and finite:
    //
    //   pool state: freshly seeded (buffers of DEFAULT_ALIGNED_BUFFER_CAPACITY
    //               available) vs. exhausted (fallback allocation path)
    //   len:        0 (the historically panicking case), 1, and a spread of
    //               sizes that cross the default pooled capacity and the
    //               16-byte alignment boundary
    //   offset:     0 and non-zero, exercised via `from_pooled_buffer_range`
    //               directly (the entry point the len==0 special-case lives
    //               in), always with a matching zero-length range so the
    //               bounds check (`end > buf_len`) stays satisfied
    //
    // For every combination, `from_pooled_buffer_range` must never panic,
    // and the resulting `len()`/emptiness must match what was requested.
    #[test]
    fn from_pooled_buffer_range_never_panics_across_pool_states_and_lens() {
        let lens = [0usize, 1, 2, 15, 16, 17, 63, 64, 65, 256, 257, 1024];

        for &initial_pool_size in &[1usize, 4] {
            for &exhaust in &[false, true] {
                for &len in &lens {
                    let pool = Arc::new(AlignedBytesPool::new(initial_pool_size));
                    let mut held = Vec::new();
                    if exhaust {
                        // Drain every slot and hold onto the buffers so the
                        // queue is genuinely empty, forcing `get_buffer` down
                        // the `AlignedBuffer::with_capacity` fallback path
                        // for the request under test.
                        while pool.available_count() > 0 {
                            held.push(pool.get_buffer(64));
                        }
                        assert_eq!(pool.available_count(), 0);
                    }

                    // Path 1: the real call chain from `start_stream_with_correlation`
                    // (`with_len` -> `into_aligned_bytes` -> `from_pooled_buffer`),
                    // covering offset == 0.
                    let pooled = PooledAlignedBuffer::with_len(len, pool.clone());
                    let bytes = AlignedBytes::from_pooled_buffer(pooled);
                    assert_eq!(
                        bytes.len(),
                        len,
                        "pool_size={initial_pool_size} exhaust={exhaust} len={len}: len mismatch"
                    );
                    assert_eq!(bytes.is_empty(), len == 0);
                    assert_eq!(bytes.as_ref().len(), len);

                    // Path 2: `from_pooled_buffer_range` with a non-zero
                    // offset and a zero-length slice at the end of the
                    // buffer, the shape used by chunked/partial reads.
                    let pooled_for_range = PooledAlignedBuffer::with_len(len, pool.clone());
                    let ranged = AlignedBytes::from_pooled_buffer_range(pooled_for_range, len, 0)
                        .unwrap_or_else(|e| {
                            panic!(
                                "pool_size={initial_pool_size} exhaust={exhaust} len={len}: \
                                 zero-length range at end-of-buffer must not error: {e:?}"
                            )
                        });
                    assert_eq!(ranged.len(), 0);
                    assert!(ranged.is_empty());

                    drop(held);
                }
            }
        }
    }

    #[test]
    fn aligned_pool_idle_retention_respects_byte_budget() {
        let budget = 4 * 1024 * 1024;
        let pool = AlignedBytesPool::new_with_budget(1024, budget);
        let mut held = Vec::new();
        for _ in 0..32 {
            let mut buf = pool.get_buffer(512 * 1024);
            buf.resize(512 * 1024, 7);
            held.push(buf);
        }
        drop(held);
        assert!(
            pool.retained_bytes() <= budget,
            "idle aligned pool retained {} over budget {}",
            pool.retained_bytes(),
            budget
        );
    }
}
