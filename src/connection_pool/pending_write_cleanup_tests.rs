//! Regression matrix for the shared pending-write state machine and adapters.
use super::*;
use std::collections::VecDeque;

#[derive(Clone, Copy)]
enum Step {
    Pending,
    Write(usize),
    Error,
}

struct ScriptedWriter {
    vectored: bool,
    steps: VecDeque<Step>,
    bytes: Vec<u8>,
    polls: usize,
    waker: Option<std::task::Waker>,
}

impl ScriptedWriter {
    fn new(vectored: bool, steps: impl IntoIterator<Item = Step>) -> Self {
        Self {
            vectored,
            steps: steps.into_iter().collect(),
            bytes: Vec::new(),
            polls: 0,
            waker: None,
        }
    }

    fn write_slices(
        &mut self,
        cx: &mut Context<'_>,
        slices: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        self.polls += 1;
        match self.steps.pop_front().unwrap_or(Step::Write(3)) {
            Step::Pending => {
                self.waker = Some(cx.waker().clone());
                Poll::Pending
            }
            Step::Error => Poll::Ready(Err(std::io::Error::other("scripted failure"))),
            Step::Write(cap) => {
                let mut remaining = cap;
                for slice in slices {
                    let n = remaining.min(slice.len());
                    self.bytes.extend_from_slice(&slice[..n]);
                    remaining -= n;
                    if remaining == 0 {
                        break;
                    }
                }
                Poll::Ready(Ok(cap - remaining))
            }
        }
    }
}

impl AsyncWrite for ScriptedWriter {
    fn is_write_vectored(&self) -> bool {
        self.vectored
    }
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.write_slices(cx, &[std::io::IoSlice::new(bytes)])
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        slices: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        self.write_slices(cx, slices)
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn fixture(kind: usize) -> (PendingOrdinaryWrite, Vec<u8>) {
    let mut header = [0; 16];
    header[..4].copy_from_slice(b"HEAD");
    let expected = b"HEADdata".to_vec();
    let pending = match kind {
        0 => PendingOrdinaryWrite::HeaderInline {
            header,
            header_len: 4,
            payload: bytes::Bytes::from_static(b"data"),
            header_off: 0,
            payload_off: 0,
        },
        1 => PendingOrdinaryWrite::HeaderInlineAligned {
            header,
            header_len: 4,
            payload: crate::AlignedBytes::from_pooled_slice(
                b"data",
                Arc::new(crate::AlignedBytesPool::default()),
            ),
            header_off: 0,
            payload_off: 0,
        },
        2 => PendingOrdinaryWrite::Chunks {
            chunks: vec![
                bytes::Bytes::new(),
                bytes::Bytes::from_static(b"HEAD"),
                bytes::Bytes::new(),
                bytes::Bytes::from_static(b"data"),
                bytes::Bytes::new(),
            ],
            offset: 0,
        },
        3 => PendingOrdinaryWrite::Buf {
            buf: Box::new(
                bytes::Bytes::from_static(b"HEAD").chain(bytes::Bytes::from_static(b"data")),
            ),
        },
        4 => {
            let header = crate::framing::write_ask_nack_header(
                17,
                crate::framing::AskNackReason::Backpressure,
            );
            return (
                PendingOrdinaryWrite::AskNack { header, offset: 0 },
                header.to_vec(),
            );
        }
        _ => unreachable!(),
    };
    (pending, expected)
}

fn remaining(write: &PendingOrdinaryWrite) -> usize {
    match write {
        PendingOrdinaryWrite::HeaderInline {
            header_len,
            payload,
            header_off,
            payload_off,
            ..
        } => header_len - header_off + payload.len() - payload_off,
        PendingOrdinaryWrite::HeaderInlineAligned {
            header_len,
            payload,
            header_off,
            payload_off,
            ..
        } => header_len - header_off + payload.len() - payload_off,
        PendingOrdinaryWrite::Chunks { chunks, offset } => {
            chunks.iter().map(|c| c.len()).sum::<usize>() - offset
        }
        PendingOrdinaryWrite::Buf { buf } => buf.remaining(),
        PendingOrdinaryWrite::AskNack { header, offset } => header.len() - offset,
    }
}

#[tokio::test]
async fn all_variants_preserve_progress_across_pending_and_short_writes() {
    for vectored in [false, true] {
        for kind in 0..5 {
            let (mut pending, expected) = fixture(kind);
            let mut writer = ScriptedWriter::new(
                vectored,
                [
                    Step::Pending,
                    Step::Write(1),
                    Step::Pending,
                    Step::Write(3),
                    Step::Write(2),
                ],
            );
            let mut committed = 0;
            for _ in 0..32 {
                let before = remaining(&pending);
                let polls = writer.polls;
                let progress = poll_pending_ordinary_nowait(&mut writer, &mut pending).await;
                assert!(
                    writer.polls <= polls + 1,
                    "nowait must issue at most one poll"
                );
                match progress {
                    OrdinaryWriteProgress::Partial(n) => {
                        committed += n;
                        assert_eq!(remaining(&pending), before - n);
                    }
                    OrdinaryWriteProgress::Complete(n) => {
                        committed += n;
                        assert_eq!(remaining(&pending), 0);
                        break;
                    }
                    OrdinaryWriteProgress::Failed => panic!("healthy scripted writer failed"),
                }
            }
            assert_eq!(committed, expected.len());
            assert_eq!(writer.bytes, expected);
            assert!(
                writer.waker.is_some(),
                "Pending must register the owner's waker"
            );
        }
    }
}

#[tokio::test]
async fn zero_and_error_do_not_commit_or_spin_for_any_variant() {
    for vectored in [false, true] {
        for kind in 0..5 {
            for failure in [Step::Write(0), Step::Error] {
                let (mut pending, _) = fixture(kind);
                let mut writer = ScriptedWriter::new(vectored, [Step::Write(1), failure]);
                assert!(matches!(
                    poll_pending_ordinary_nowait(&mut writer, &mut pending).await,
                    OrdinaryWriteProgress::Partial(1)
                ));
                let before = remaining(&pending);
                assert!(matches!(
                    poll_pending_ordinary_nowait(&mut writer, &mut pending).await,
                    OrdinaryWriteProgress::Failed
                ));
                assert_eq!(remaining(&pending), before);
                assert_eq!(writer.bytes.len(), 1);
                assert_eq!(writer.polls, 2);
            }
        }
    }
}

#[tokio::test]
async fn cancelled_wait_retains_offsets_and_can_resume_for_every_variant() {
    for vectored in [false, true] {
        for kind in 0..5 {
            let (mut pending, expected) = fixture(kind);
            let mut writer = ScriptedWriter::new(vectored, [Step::Write(1), Step::Pending]);
            assert!(matches!(
                poll_pending_ordinary_nowait(&mut writer, &mut pending).await,
                OrdinaryWriteProgress::Partial(1)
            ));
            let before = remaining(&pending);
            let mut waiting = Box::pin(wait_pending_ordinary(&mut writer, &mut pending));
            assert!(futures::poll!(waiting.as_mut()).is_pending());
            drop(waiting);
            assert_eq!(remaining(&pending), before);
            writer
                .waker
                .take()
                .expect("waiting poll must register waker")
                .wake();
            for _ in 0..32 {
                if matches!(
                    wait_pending_ordinary(&mut writer, &mut pending).await,
                    OrdinaryWriteProgress::Complete(_)
                ) {
                    break;
                }
            }
            assert_eq!(remaining(&pending), 0);
            assert_eq!(writer.bytes, expected);
        }
    }
}

#[tokio::test]
async fn completed_and_empty_chunks_need_no_write_poll() {
    let mut writer = ScriptedWriter::new(true, [Step::Error]);
    let mut pending = PendingOrdinaryWrite::Chunks {
        chunks: vec![bytes::Bytes::new()],
        offset: 0,
    };
    assert!(matches!(
        poll_pending_ordinary_nowait(&mut writer, &mut pending).await,
        OrdinaryWriteProgress::Complete(0)
    ));
    assert_eq!(writer.polls, 0);
}

/// Synthetic scheduling profile, not a socket/allocator benchmark. Run against
/// both snapshots with the same harness; assertions keep timing byte-correct.
#[tokio::test]
#[ignore = "profiling-only; timings are evidence, not a performance gate"]
async fn profile_chunk_write_backpressure() {
    const SAMPLES: usize = 500;
    for (label, chunks_count, empty_padding) in [
        ("one_chunk", 1, false),
        ("small_chunks", 64, false),
        ("empty_interleaved", 64, true),
    ] {
        for cap in [3, 1024] {
            let bytes_per_chunk = 512 / chunks_count;
            let data = bytes::Bytes::from(vec![0xA5; bytes_per_chunk]);
            let mut times = Vec::with_capacity(SAMPLES);
            let mut polls = 0;
            for _ in 0..SAMPLES {
                let mut chunks = Vec::new();
                for _ in 0..chunks_count {
                    if empty_padding {
                        chunks.push(bytes::Bytes::new());
                    }
                    chunks.push(data.clone());
                }
                let mut pending = PendingOrdinaryWrite::Chunks { chunks, offset: 0 };
                let steps = (0..(512 / cap + 2)).flat_map(|_| [Step::Write(cap), Step::Pending]);
                let mut writer = ScriptedWriter::new(true, steps);
                writer.bytes.reserve(512);
                let start = std::time::Instant::now();
                let mut committed = 0;
                for _ in 0..1024 {
                    match poll_pending_ordinary_nowait(&mut writer, &mut pending).await {
                        OrdinaryWriteProgress::Complete(n) => {
                            committed += n;
                            break;
                        }
                        OrdinaryWriteProgress::Partial(n) => committed += n,
                        OrdinaryWriteProgress::Failed => panic!("profile writer failed"),
                    }
                }
                times.push(start.elapsed().as_nanos());
                polls += writer.polls;
                assert_eq!(committed, 512);
                assert_eq!(remaining(&pending), 0);
                assert_eq!(writer.bytes, vec![0xA5; 512]);
            }
            times.sort_unstable();
            let sum: u128 = times.iter().sum();
            println!(
                "CHUNK_PROFILE workload={label} write_cap={cap} samples={SAMPLES} total_ns={sum} p50_ns={} p99_ns={} polls={polls}",
                times[SAMPLES / 2],
                times[SAMPLES * 99 / 100]
            );
        }
    }
}
