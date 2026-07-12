/// Vectored write command for zero-copy header + payload operations
#[derive(Debug)]
pub struct VectoredSendItem {
    header: bytes::Bytes,
    payload: bytes::Bytes,
}
