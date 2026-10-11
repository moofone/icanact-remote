//! Bounded stack scatter polling retains ordering across multiple IO turns.
use super::*;

struct ChunkPollWriter {
    bytes: Vec<u8>,
    max_slices: usize,
    pending: bool,
}

impl crate::connection_pool::outcome_window::OutcomeWrite for ChunkPollWriter {}

impl AsyncWrite for ChunkPollWriter {
    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.poll_write_vectored(cx, &[std::io::IoSlice::new(bytes)])
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        slices: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        self.max_slices = self.max_slices.max(slices.len());
        if self.pending {
            self.pending = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.pending = true;
        let mut count = 0;
        // End inside a three-byte chunk to test offset conservation too.
        for slice in slices {
            let n = slice.len().min(191 - count);
            self.bytes.extend_from_slice(&slice[..n]);
            count += n;
            if count == 191 {
                break;
            }
        }
        Poll::Ready(Ok(count))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn more_than_sixty_four_chunks_keep_order_across_pending_and_short_writes() {
    let mut chunks = Vec::new();
    let mut expected = Vec::new();
    for byte in 0..130u8 {
        chunks.push(bytes::Bytes::new());
        chunks.push(bytes::Bytes::from(vec![byte; 3]));
        expected.extend_from_slice(&[byte; 3]);
    }
    let mut pending = PendingOrdinaryWrite::chunks(chunks.into(), 0);
    let mut writer = ChunkPollWriter {
        bytes: Vec::new(),
        max_slices: 0,
        pending: true,
    };
    let mut saw_pending = false;
    let mut complete = false;
    for _ in 0..16 {
        match poll_pending_ordinary_nowait(&mut writer, &mut pending).await {
            OrdinaryWriteProgress::Partial(0) => saw_pending = true,
            OrdinaryWriteProgress::Partial(_) => {}
            OrdinaryWriteProgress::Complete(_) => {
                complete = true;
                break;
            }
            OrdinaryWriteProgress::Failed => panic!("healthy chunk writer failed"),
        }
    }
    assert!(complete, "all chunks must make bounded progress");
    assert!(saw_pending, "the pending control must actually run");
    assert_eq!(writer.max_slices, 64);
    assert_eq!(writer.bytes, expected);
}
