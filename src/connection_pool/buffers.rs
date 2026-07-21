/// Fixed-capacity wire header stored inline in a streaming command. V5's
/// largest header is 32 bytes, so this avoids one heap allocation per chunk.
#[derive(Debug, Clone, Copy)]
pub struct InlineFrameHeader {
    bytes: [u8; 32],
    len: u8,
}

impl InlineFrameHeader {
    pub fn from_array<const N: usize>(header: [u8; N]) -> Self {
        assert!(N <= 32, "V5 inline header exceeds fixed capacity");
        let mut bytes = [0u8; 32];
        bytes[..N].copy_from_slice(&header);
        Self { bytes, len: N as u8 }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

}

/// Vectored write command for zero-copy header + payload operations.
#[derive(Debug)]
pub struct VectoredSendItem {
    header: InlineFrameHeader,
    payload: bytes::Bytes,
}
