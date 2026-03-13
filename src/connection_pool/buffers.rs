/// Memory pool for zero-allocation message handling
#[derive(Debug)]
pub struct MessageBufferPool {
    queue: crossbeam_queue::ArrayQueue<Vec<u8>>,
}

impl MessageBufferPool {
    pub fn new(pool_size: usize, buffer_size: usize) -> Self {
        let pool_size = pool_size.max(1);
        let queue = crossbeam_queue::ArrayQueue::new(pool_size);
        for _ in 0..pool_size {
            let _ = queue.push(Vec::with_capacity(buffer_size));
        }
        Self { queue }
    }

    /// Get a buffer from the pool - returns None if pool is empty
    pub fn get_buffer(&self) -> Option<Vec<u8>> {
        self.queue.pop().map(|mut buffer| {
            buffer.clear();
            buffer
        })
    }

    /// Return a buffer to the pool
    pub fn return_buffer(&self, mut buffer: Vec<u8>) {
        buffer.clear(); // Reset length but keep capacity
        let _ = self.queue.push(buffer);
    }

    #[allow(dead_code)]
    pub fn available_count(&self) -> usize {
        self.queue.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.available_count() == 0
    }
}

/// Vectored write command for zero-copy header + payload operations
#[derive(Debug)]
pub struct VectoredSendItem {
    header: bytes::Bytes,
    payload: bytes::Bytes,
}
