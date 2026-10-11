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
/// Aggregate live-plus-idle capacity budget for the aligned receive pool,
/// shared by every connection on a registry. Checkout is rejected before
/// allocation when this bound or the configured pool count is exhausted.
pub const DEFAULT_ALIGNED_POOL_BYTE_BUDGET: usize = 64 * 1024 * 1024;

pub type AlignedBuffer = AlignedVec<PAYLOAD_ALIGNMENT>;

/// Pooled aligned buffer owner for Bytes::from_owner.
#[derive(Debug)]
pub struct PooledAlignedBuffer {
    buffer: AlignedBuffer,
    pool: Arc<AlignedBytesPool>,
}

impl PooledAlignedBuffer {
    pub fn with_len(len: usize, pool: Arc<AlignedBytesPool>) -> Result<Self, GossipError> {
        // This is used by framed receive paths which take `&mut [u8]` slices.
        // For safety, the slice must point to initialized memory, so we resize (zero-fill).
        // We still reuse the pooled allocation to avoid per-message heap allocs.
        let mut buffer = pool.acquire_buffer(len)?;
        if buffer.len() != len {
            buffer.resize(len, 0);
        }
        Ok(Self { buffer, pool })
    }

    /// Compatibility constructor. Storage is initialized even though the old
    /// name suggests otherwise: this owner exposes ordinary byte slices, so it
    /// cannot soundly represent uninitialized bytes.
    ///
    /// # Safety
    ///
    /// No additional caller obligations. The legacy unsafe marker is retained;
    /// its result is now fallible. New callers should
    /// use [`Self::with_len`].
    pub unsafe fn with_len_uninit(
        len: usize,
        pool: Arc<AlignedBytesPool>,
    ) -> Result<Self, GossipError> {
        Self::with_len(len, pool)
    }

    pub fn from_slice(data: &[u8], pool: Arc<AlignedBytesPool>) -> Result<Self, GossipError> {
        let mut buffer = pool.acquire_buffer(data.len())?;
        buffer.extend_from_slice(data);
        Ok(Self { buffer, pool })
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
    OwnedAligned(AlignedBuffer),
    OwnedBytes {
        bytes: Bytes,
        retained_capacity: usize,
    },
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
        // REMOTE-3: as with pooled views, an empty view has no archived bytes
        // to dereference and does not impose alignment on its dangling pointer.
        if !bytes.is_empty() && (bytes.as_ptr() as usize) % PAYLOAD_ALIGNMENT != 0 {
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
        let len = vec.len();
        Self {
            inner: AlignedBytesInner::OwnedAligned(vec),
            offset: 0,
            len,
        }
    }

    /// Explicit sharing boundary for callers that need borrowed `slice` views.
    /// Native aligned owners otherwise stay unique and allocation-free when
    /// moved into forwarding. Sharing may allocate Bytes owner metadata.
    pub fn into_shared(self) -> Self {
        if matches!(
            &self.inner,
            AlignedBytesInner::Bytes(_) | AlignedBytesInner::OwnedBytes { .. }
        ) {
            return self;
        }
        let retained_capacity = self
            .retained_capacity()
            .expect("native aligned owners carry capacity");
        Self::from_owned_bytes(self.into_bytes(), retained_capacity)
            .expect("the current aligned view remains aligned")
    }

    /// Consume a subview without cloning or promoting its complete owner.
    pub fn into_range(mut self, range: std::ops::Range<usize>) -> Result<Self, GossipError> {
        if range.start > range.end || range.end > self.len {
            return Err(GossipError::InvalidConfig(
                "payload range out of bounds".into(),
            ));
        }
        let len = range.end - range.start;
        if len != 0
            && (self.as_ref().as_ptr().wrapping_add(range.start) as usize) % PAYLOAD_ALIGNMENT != 0
        {
            return Err(GossipError::InvalidConfig(
                "misaligned payload range".into(),
            ));
        }
        self.offset += range.start;
        self.len = len;
        Ok(self)
    }

    fn from_owned_bytes(bytes: Bytes, retained_capacity: usize) -> Result<Self, GossipError> {
        // Reuse the empty/nonempty alignment rule without losing the receipt.
        let aligned = Self::from_bytes(bytes)?;
        let len = aligned.len;
        Ok(Self {
            inner: AlignedBytesInner::OwnedBytes {
                bytes: aligned.into_bytes(),
                retained_capacity,
            },
            offset: 0,
            len,
        })
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
        // R5: an empty view has no archived bytes to dereference. A legal
        // zero-capacity owner can carry a dangling, non-16-aligned pointer.
        // Nonempty payloads still require the full transport alignment.
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

    pub fn from_pooled_slice(
        data: &[u8],
        pool: Arc<AlignedBytesPool>,
    ) -> Result<Self, GossipError> {
        let buffer = PooledAlignedBuffer::from_slice(data, pool)?;
        Ok(Self::from_pooled_buffer(buffer))
    }

    pub(crate) fn retained_capacity(&self) -> Option<usize> {
        match &self.inner {
            AlignedBytesInner::OwnedAligned(owner) => Some(owner.capacity()),
            AlignedBytesInner::Pooled(owner) => Some(owner.buffer.capacity()),
            AlignedBytesInner::OwnedBytes {
                retained_capacity, ..
            } => Some(*retained_capacity),
            AlignedBytesInner::Bytes(_) => None,
        }
    }

    /// Explicit compatibility promotion. Native owners may allocate shared
    /// metadata; zero-copy owner transfers should retain AlignedBytes itself.
    pub fn into_bytes(self) -> Bytes {
        let bytes = match self.inner {
            AlignedBytesInner::Bytes(bytes) | AlignedBytesInner::OwnedBytes { bytes, .. } => bytes,
            AlignedBytesInner::OwnedAligned(buffer) => Bytes::from_owner(buffer),
            AlignedBytesInner::Pooled(buffer) => Bytes::from_owner(buffer),
        };
        bytes.slice(self.offset..self.offset + self.len)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Borrowed views require explicit `into_shared` for native owners. Use
    /// `into_range` to move a unique owner without metadata allocation.
    pub fn slice(&self, range: std::ops::Range<usize>) -> Result<Self, GossipError> {
        if range.start > range.end || range.end > self.len {
            return Err(GossipError::InvalidConfig(
                "payload range out of bounds".into(),
            ));
        }
        let range = self.offset + range.start..self.offset + range.end;
        match &self.inner {
            AlignedBytesInner::Bytes(bytes) => Self::from_bytes(bytes.slice(range)),
            AlignedBytesInner::OwnedBytes {
                bytes,
                retained_capacity,
            } => Self::from_owned_bytes(bytes.slice(range), *retained_capacity),
            AlignedBytesInner::OwnedAligned(_) | AlignedBytesInner::Pooled(_) => {
                Err(GossipError::InvalidConfig(
                    "borrowed slices require explicit sharing; use into_range for unique owners"
                        .into(),
                ))
            }
        }
    }
}

impl AsRef<[u8]> for AlignedBytes {
    fn as_ref(&self) -> &[u8] {
        let bytes = match &self.inner {
            AlignedBytesInner::Bytes(bytes) | AlignedBytesInner::OwnedBytes { bytes, .. } => {
                bytes.as_ref()
            }
            AlignedBytesInner::OwnedAligned(buffer) => buffer.as_ref(),
            AlignedBytesInner::Pooled(buffer) => buffer.as_ref(),
        };
        &bytes[self.offset..self.offset + self.len]
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
    live_bytes: AtomicUsize,
    checked_out: AtomicUsize,
    checkout_limit: usize,
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
            live_bytes: AtomicUsize::new(0),
            checked_out: AtomicUsize::new(0),
            checkout_limit: pool_size.max(1),
        };
        for _ in 0..pool_size {
            if !pool.try_reserve(DEFAULT_ALIGNED_BUFFER_CAPACITY) {
                break;
            }
            let buffer = AlignedBuffer::with_capacity(DEFAULT_ALIGNED_BUFFER_CAPACITY);
            let cap = buffer.capacity();
            if cap > DEFAULT_ALIGNED_BUFFER_CAPACITY {
                drop(buffer);
                pool.release_reservation(DEFAULT_ALIGNED_BUFFER_CAPACITY);
                break;
            }
            pool.release_reservation(DEFAULT_ALIGNED_BUFFER_CAPACITY - cap);
            pool.live_bytes.fetch_add(cap, Ordering::AcqRel);
            if pool.queue.push(buffer).is_err() {
                pool.live_bytes.fetch_sub(cap, Ordering::AcqRel);
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

    /// Checked checkout. Count and live-plus-idle capacity are bounded by
    /// construction; saturation returns `WriteQueueFull` before allocation.
    /// The lease remains charged until its last owner (including Bytes views)
    /// drops. It cannot be forcibly recycled while application code borrows it.
    pub fn get_buffer(self: &Arc<Self>, len: usize) -> Result<PooledAlignedBuffer, GossipError> {
        PooledAlignedBuffer::with_len(len, self.clone())
    }

    fn acquire_buffer(&self, min_capacity: usize) -> Result<AlignedBuffer, GossipError> {
        if min_capacity > self.budget {
            return Err(GossipError::WriteQueueFull);
        }
        let previous = self.checked_out.fetch_add(1, Ordering::AcqRel);
        if previous >= self.checkout_limit {
            self.checked_out.fetch_sub(1, Ordering::AcqRel);
            return Err(GossipError::WriteQueueFull);
        }
        let mut buffer = self.queue.pop().unwrap_or_default();
        self.release_reservation(buffer.capacity());
        if buffer.capacity() < min_capacity {
            // Cold size mismatch: reuse an adequate idle owner before growing.
            // Keeping undersized seeds ahead of warm buffers can otherwise
            // exhaust the byte budget even with reusable storage sitting idle.
            // Bound the scan; a warm checkout retains the original fast path.
            for _ in 0..self.checkout_limit {
                if buffer.capacity() >= min_capacity {
                    break;
                }
                let Some(next) = self.queue.pop() else { break };
                let old_capacity = buffer.capacity();
                drop(buffer);
                self.live_bytes.fetch_sub(old_capacity, Ordering::AcqRel);
                buffer = next;
                self.release_reservation(buffer.capacity());
            }
            if buffer.capacity() < min_capacity {
                let old_capacity = buffer.capacity();
                drop(buffer);
                self.live_bytes.fetch_sub(old_capacity, Ordering::AcqRel);
                let Some(class) = min_capacity.checked_next_power_of_two() else {
                    self.return_buffer(AlignedBuffer::new());
                    return Err(GossipError::WriteQueueFull);
                };
                let reserved = self
                    .live_bytes
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                        current
                            .checked_add(class)
                            .filter(|next| *next <= self.budget)
                    })
                    .is_ok();
                if !reserved {
                    self.return_buffer(AlignedBuffer::new());
                    return Err(GossipError::WriteQueueFull);
                }
                buffer = AlignedBuffer::with_capacity(min_capacity);
                let actual = buffer.capacity();
                if actual > class {
                    drop(buffer);
                    self.live_bytes.fetch_sub(class, Ordering::AcqRel);
                    self.return_buffer(AlignedBuffer::new());
                    return Err(GossipError::WriteQueueFull);
                }
                self.live_bytes.fetch_sub(class - actual, Ordering::AcqRel);
            }
        }
        buffer.clear();
        Ok(buffer)
    }

    fn return_buffer(&self, mut buffer: AlignedBuffer) {
        buffer.clear();
        let cap = buffer.capacity();
        if cap <= MAX_POOLED_ALIGNED_CAPACITY && self.try_reserve(cap) {
            if self.queue.push(buffer).is_err() {
                self.release_reservation(cap);
                self.live_bytes.fetch_sub(cap, Ordering::AcqRel);
            }
        } else {
            drop(buffer);
            self.live_bytes.fetch_sub(cap, Ordering::AcqRel);
        }
        self.checked_out.fetch_sub(1, Ordering::AcqRel);
    }

    /// Total capacity owned by this pool, including checked-out owners.
    pub fn live_bytes(&self) -> usize {
        self.live_bytes.load(Ordering::Acquire)
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

/// A legal reader may inspect any initialized byte before replacing it. This
/// probe also fragments reads and alternates Pending to test resumable storage.
#[cfg(test)]
pub(crate) struct InitializedReadProbe {
    bytes: Vec<u8>,
    offset: usize,
    pending: bool,
}

#[cfg(test)]
impl InitializedReadProbe {
    pub(crate) fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            offset: 0,
            pending: false,
        }
    }
}

#[cfg(test)]
impl tokio::io::AsyncRead for InitializedReadProbe {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        assert!(
            buf.initialized()[buf.filled().len()..]
                .iter()
                .all(|byte| *byte == 0)
        );
        if self.pending {
            self.pending = false;
            cx.waker().wake_by_ref();
            return std::task::Poll::Pending;
        }
        if self.offset < self.bytes.len() && buf.remaining() > 0 {
            buf.put_slice(&self.bytes[self.offset..self.offset + 1]);
            self.offset += 1;
            self.pending = true;
        }
        std::task::Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aligned_vec_owner_receipt_survives_tiny_and_empty_slices() {
        let mut owner = AlignedBuffer::with_capacity(1024);
        owner.resize(32, 7);
        let capacity = owner.capacity();
        let pointer = owner.as_ptr();
        let payload = AlignedBytes::from_aligned_vec(owner).into_shared();
        assert_eq!(payload.retained_capacity(), Some(capacity));
        let tiny = payload.slice(16..17).unwrap();
        assert_eq!(tiny.retained_capacity(), Some(capacity));
        assert_eq!(tiny.as_ref().as_ptr(), pointer.wrapping_add(16));
        let forwarded = crate::ForwardPayload::try_from(tiny).unwrap();
        assert_eq!(forwarded.retained_bytes(), capacity);
        assert_eq!(forwarded.as_ref(), &[7]);
        let empty = payload.slice(1..1).unwrap();
        assert_eq!(empty.retained_capacity(), Some(capacity));
        assert!(empty.is_empty());
        assert!(payload.slice(1..2).is_err());
    }

    #[test]
    fn empty_nonpooled_aligned_bytes_and_vec_are_valid() {
        let empty = AlignedBytes::from_bytes(Bytes::new()).expect("empty Bytes");
        assert!(empty.is_empty());
        assert_eq!(empty.as_ref(), b"");
        let empty_vec = AlignedBytes::from_aligned_vec(AlignedBuffer::new());
        assert!(empty_vec.is_empty());
        assert_eq!(empty_vec.into_bytes().as_ref(), b"");
    }

    #[test]
    fn empty_owner_view_accepts_misalignment_but_nonempty_view_rejects_it() {
        struct OffsetOwner {
            storage: AlignedBuffer,
            len: usize,
        }
        impl AsRef<[u8]> for OffsetOwner {
            fn as_ref(&self) -> &[u8] {
                &self.storage[1..1 + self.len]
            }
        }
        for len in [0, 1] {
            let mut storage = AlignedBuffer::with_capacity(PAYLOAD_ALIGNMENT);
            storage.resize(PAYLOAD_ALIGNMENT, 7);
            let bytes = Bytes::from_owner(OffsetOwner { storage, len });
            assert_eq!(bytes.len(), len);
            assert_ne!(bytes.as_ptr() as usize % PAYLOAD_ALIGNMENT, 0);
            let result = AlignedBytes::from_bytes(bytes);
            if len == 0 {
                let empty = result.expect("empty misaligned owner view");
                assert!(empty.is_empty());
                assert_eq!(empty.into_bytes().as_ref(), b"");
            } else {
                assert!(result.is_err(), "nonempty misalignment must stay rejected");
            }
        }
        let mut aligned = AlignedBuffer::with_capacity(PAYLOAD_ALIGNMENT);
        aligned.extend_from_slice(b"aligned");
        assert_eq!(AlignedBytes::from_aligned_vec(aligned).as_ref(), b"aligned");
    }

    #[test]
    fn receive_storage_is_initialized_after_exhaustion_growth_and_reuse() {
        let pool = Arc::new(AlignedBytesPool::new(1));
        let held = pool.get_buffer(64).unwrap();
        assert!(matches!(
            pool.get_buffer(1),
            Err(GossipError::WriteQueueFull)
        ));
        drop(held);
        // Exhaustion rejects before allocation; returned capacity is reusable.
        for len in [0, 1, 257, 4096, 32] {
            let mut buffer = PooledAlignedBuffer::with_len(len, pool.clone()).unwrap();
            assert_eq!(buffer.as_ref(), vec![0; len]);
            buffer.as_mut_slice().fill(0xa5);
            drop(buffer);
            // SAFETY: the compatibility constructor has no caller obligations.
            let compat =
                unsafe { PooledAlignedBuffer::with_len_uninit(len, pool.clone()) }.unwrap();
            assert_eq!(compat.as_ref(), vec![0; len]);
        }
    }

    #[test]
    fn aligned_bytes_pool_reuses_buffers_and_alignment() {
        let pool = Arc::new(AlignedBytesPool::new(2));
        assert_eq!(pool.available_count(), 2);

        let bytes = AlignedBytes::from_pooled_slice(&[1u8, 2, 3, 4], pool.clone()).unwrap();
        let ptr = bytes.as_ref().as_ptr() as usize;
        assert_eq!(ptr % PAYLOAD_ALIGNMENT, 0);
        drop(bytes);

        assert_eq!(pool.available_count(), 2);
    }

    // R5 remains enforced after admission migration: saturation returns an
    // error, then an empty checkout succeeds safely once an owner is returned.
    #[test]
    fn empty_pool_zero_len_pooled_buffer_does_not_panic() {
        // ArrayQueue requires non-zero capacity, so model "pool exhausted
        // under load" the same way production traffic reaches it: check the
        // single slot out and hold onto it.
        let pool = Arc::new(AlignedBytesPool::new(1));
        let checked_out = pool.get_buffer(64).unwrap();
        assert_eq!(pool.available_count(), 0, "pool must be exhausted");

        assert!(PooledAlignedBuffer::with_len(0, pool.clone()).is_err());
        drop(checked_out);
        let pooled = PooledAlignedBuffer::with_len(0, pool).unwrap();
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
    //               available) vs. exhausted (typed rejection followed by release)
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
                        // queue is genuinely empty. Admission must reject,
                        // then recover once these owners are dropped.
                        while pool.available_count() > 0 {
                            held.push(pool.get_buffer(64).unwrap());
                        }
                        assert_eq!(pool.available_count(), 0);
                        assert!(pool.get_buffer(len).is_err());
                        held.clear();
                    }

                    // Path 1: the real call chain from `start_stream_with_correlation`
                    // (`with_len` -> `into_aligned_bytes` -> `from_pooled_buffer`),
                    // covering offset == 0.
                    let pooled = PooledAlignedBuffer::with_len(len, pool.clone()).unwrap();
                    let bytes = AlignedBytes::from_pooled_buffer(pooled);
                    assert_eq!(
                        bytes.len(),
                        len,
                        "pool_size={initial_pool_size} exhaust={exhaust} len={len}: len mismatch"
                    );
                    assert_eq!(bytes.is_empty(), len == 0);
                    assert_eq!(bytes.as_ref().len(), len);

                    drop(bytes);

                    // Path 2: `from_pooled_buffer_range` with a non-zero
                    // offset and a zero-length slice at the end of the
                    // buffer, the shape used by chunked/partial reads.
                    let pooled_for_range =
                        PooledAlignedBuffer::with_len(len, pool.clone()).unwrap();
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
        let pool = Arc::new(AlignedBytesPool::new_with_budget(32, budget));
        let mut held = Vec::new();
        while let Ok(buffer) = pool.get_buffer(512 * 1024) {
            held.push(buffer);
        }
        assert!(!held.is_empty());
        assert!(held.len() < 32);
        assert!(pool.live_bytes() <= budget);
        drop(held);
        assert!(pool.retained_bytes() <= budget);
        assert!(pool.retained_bytes() > 0);
        assert!(pool.get_buffer(budget + 1).is_err());
        assert!(pool.get_buffer(64).is_ok());
    }

    #[test]
    fn idle_seeds_do_not_block_a_larger_checkout_at_the_byte_bound() {
        let pool = Arc::new(AlignedBytesPool::new_with_budget(1024, 64 * 1024));
        assert_eq!(pool.live_bytes(), 64 * 1024);
        let larger = pool.get_buffer(32 * 1024).unwrap();
        assert_eq!(larger.as_ref().len(), 32 * 1024);
        assert!(pool.live_bytes() <= 64 * 1024);
        drop(larger);
        assert!(pool.retained_bytes() <= 64 * 1024);
    }

    #[test]
    fn adequate_idle_owner_behind_small_seed_is_reused_without_allocation() {
        use crate::ask_forwarder::allocation_evidence;
        let pool = Arc::new(AlignedBytesPool::new_with_budget(4, 768));
        let small = pool.get_buffer(64).unwrap();
        let large = pool.get_buffer(512).unwrap();
        let original = large.as_ref().as_ptr();
        drop(small);
        drop(large);
        let (reused, counts) = allocation_evidence::measure(|| pool.get_buffer(512).unwrap());
        assert_eq!(reused.as_ref().as_ptr(), original);
        assert_eq!(counts.allocations, 0);
        assert_eq!(counts.reallocations, 0);
        assert!(pool.live_bytes() <= 768);
        assert!(matches!(
            pool.get_buffer(512),
            Err(GossipError::WriteQueueFull)
        ));
        drop(reused);
        assert!(pool.get_buffer(512).is_ok());
        assert!(pool.retained_bytes() <= 768);
    }

    #[test]
    fn aligned_live_checkout_exhaustion_has_no_fallback_allocation() {
        let pool = Arc::new(AlignedBytesPool::new_with_budget(1, 1024));
        let first = pool.get_buffer(512).unwrap();
        let before = pool.live_bytes();
        assert!(matches!(
            pool.get_buffer(1),
            Err(GossipError::WriteQueueFull)
        ));
        assert_eq!(pool.live_bytes(), before);
        let bytes = first.into_aligned_bytes().into_bytes();
        let view = bytes.slice(0..1);
        drop(bytes);
        assert!(
            pool.get_buffer(1).is_err(),
            "a sliced owner still holds admission"
        );
        drop(view);
        assert!(pool.get_buffer(512).is_ok());
        assert!(pool.get_buffer(MAX_POOLED_ALIGNED_CAPACITY + 1).is_err());
    }

    #[test]
    fn warm_aligned_checkout_and_saturation_have_scoped_allocation_controls() {
        use crate::ask_forwarder::allocation_evidence;

        let pool = Arc::new(AlignedBytesPool::new_with_budget(1, 1024));
        let warm = pool.get_buffer(512).unwrap();
        let original = warm.as_ref().as_ptr();
        drop(warm);
        let (held, counts) = allocation_evidence::measure(|| pool.get_buffer(512).unwrap());
        assert_eq!(held.as_ref().as_ptr(), original);
        assert_eq!(counts.allocations, 0);
        assert_eq!(counts.reallocations, 0);
        let (_, counts) = allocation_evidence::measure(|| {
            for _ in 0..32 {
                assert!(matches!(
                    pool.get_buffer(1),
                    Err(GossipError::WriteQueueFull)
                ));
            }
        });
        assert_eq!(counts.allocations, 0);
        assert_eq!(counts.reallocations, 0);
        drop(held);
        let (recovered, counts) = allocation_evidence::measure(|| pool.get_buffer(512).unwrap());
        assert_eq!(recovered.as_ref().as_ptr(), original);
        assert_eq!(counts.allocations, 0);
        assert_eq!(counts.reallocations, 0);
        let (positive, counts) =
            allocation_evidence::measure(|| std::hint::black_box(vec![0u8; 17]));
        assert!(counts.allocations > 0);
        assert!(counts.requested_bytes >= positive.len());
    }
}
