use crossbeam_queue::ArrayQueue;
use std::ops::Deref;
use std::sync::Arc;

use bytes::Bytes;
use rkyv::util::AlignedVec;

use crate::GossipError;

pub const PAYLOAD_ALIGNMENT: usize = 16;
pub const DEFAULT_ALIGNED_POOL_SIZE: usize = 64;
pub const MAX_POOLED_ALIGNED_CAPACITY: usize = 8 * 1024 * 1024; // 8MB
pub const DEFAULT_ALIGNED_BUFFER_CAPACITY: usize = 256;

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

    pub fn from_slice(data: &[u8], pool: Arc<AlignedBytesPool>) -> Self {
        let mut buffer = pool.get_buffer(data.len());
        buffer.extend_from_slice(data);
        Self { buffer, pool }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buffer.as_mut_slice()
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
    pool_size: usize,
}

impl AlignedBytesPool {
    pub fn new(pool_size: usize) -> Self {
        let queue = ArrayQueue::new(pool_size);
        for _ in 0..pool_size {
            let _ = queue.push(AlignedBuffer::with_capacity(
                DEFAULT_ALIGNED_BUFFER_CAPACITY,
            ));
        }

        Self { queue, pool_size }
    }

    /// CRITICAL_PATH: acquire aligned buffer without extra allocations.
    pub fn get_buffer(&self, min_capacity: usize) -> AlignedBuffer {
        if let Some(mut buffer) = self.queue.pop() {
            if buffer.capacity() < min_capacity {
                buffer.reserve(min_capacity - buffer.capacity());
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

        if buffer.capacity() > MAX_POOLED_ALIGNED_CAPACITY {
            return;
        }
        let _ = self.queue.push(buffer);
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
}
