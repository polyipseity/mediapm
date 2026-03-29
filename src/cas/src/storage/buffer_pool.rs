//! Reusable byte-buffer pool for incremental stream ingestion.
//!
//! The filesystem backend frequently reads streams in fixed-size chunks.
//! Reusing buffers here reduces allocation churn on hot ingestion paths while
//! keeping ownership explicit via RAII leases.

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use bytes::BytesMut;
use parking_lot::Mutex;

/// Lock-protected reusable byte buffer pool.
///
/// Buffers are normalized to a fixed `chunk_size` and recycled up to
/// `max_buffers` retained entries.
#[derive(Debug)]
pub(crate) struct StreamBufferPool {
    chunk_size: usize,
    max_buffers: usize,
    buffers: Mutex<Vec<BytesMut>>,
}

impl StreamBufferPool {
    /// Creates a new buffer pool.
    pub(crate) fn new(chunk_size: usize, max_buffers: usize) -> Arc<Self> {
        Arc::new(Self { chunk_size, max_buffers, buffers: Mutex::new(Vec::new()) })
    }

    /// Leases one buffer from the pool.
    ///
    /// Returned buffers are `BytesMut` with at least `chunk_size` capacity.
    pub(crate) fn lease(self: &Arc<Self>) -> PooledStreamBuffer {
        let mut buffer = {
            let mut buffers = self.buffers.lock();
            buffers.pop().unwrap_or_default()
        };

        if buffer.capacity() < self.chunk_size {
            buffer.reserve(self.chunk_size - buffer.capacity());
        }
        buffer.clear();

        PooledStreamBuffer { pool: Arc::clone(self), buffer: Some(buffer) }
    }

    fn release(&self, mut buffer: BytesMut) {
        buffer.clear();

        let mut buffers = self.buffers.lock();
        if buffers.len() < self.max_buffers {
            buffers.push(buffer);
        }
    }
}

/// RAII lease for one pooled stream buffer.
///
/// On drop, the buffer is returned to the originating pool unless the pool is
/// already at its retention limit.
#[derive(Debug)]
pub(crate) struct PooledStreamBuffer {
    pool: Arc<StreamBufferPool>,
    buffer: Option<BytesMut>,
}

impl Deref for PooledStreamBuffer {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        self.buffer.as_ref().expect("pooled stream buffer must be present")
    }
}

impl DerefMut for PooledStreamBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buffer.as_mut().expect("pooled stream buffer must be present")
    }
}

impl Drop for PooledStreamBuffer {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.pool.release(buffer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::StreamBufferPool;

    #[test]
    fn lease_reuses_capacity_and_clears_length() {
        let pool = StreamBufferPool::new(32 * 1024, 2);

        {
            let mut lease = pool.lease();
            lease.extend_from_slice(b"hello");
            assert_eq!(lease.len(), 5);
            assert!(lease.capacity() >= 32 * 1024);
        }

        let lease = pool.lease();
        assert_eq!(lease.len(), 0);
        assert!(lease.capacity() >= 32 * 1024);
    }
}
