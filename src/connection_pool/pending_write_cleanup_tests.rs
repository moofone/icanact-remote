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

impl crate::connection_pool::outcome_window::OutcomeWrite for ScriptedWriter {}

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
        0 => PendingOrdinaryWrite::new(PendingOrdinaryWriteKind::HeaderInline {
            header,
            header_len: 4,
            payload: bytes::Bytes::from_static(b"data"),
            header_off: 0,
            payload_off: 0,
        }),
        1 => PendingOrdinaryWrite::new(PendingOrdinaryWriteKind::HeaderInlineAligned {
            header,
            header_len: 4,
            payload: crate::AlignedBytes::from_pooled_slice(
                b"data",
                Arc::new(crate::AlignedBytesPool::default()),
            )
            .unwrap(),
            header_off: 0,
            payload_off: 0,
        }),
        2 => PendingOrdinaryWrite::chunks(
            vec![
                bytes::Bytes::new(),
                bytes::Bytes::from_static(b"HEAD"),
                bytes::Bytes::new(),
                bytes::Bytes::from_static(b"data"),
                bytes::Bytes::new(),
            ]
            .into(),
            0,
        ),
        3 => PendingOrdinaryWrite::new(PendingOrdinaryWriteKind::Buf {
            buf: Box::new(
                bytes::Bytes::from_static(b"HEAD").chain(bytes::Bytes::from_static(b"data")),
            ),
        }),
        4 => {
            let header = crate::framing::write_ask_nack_header(
                17,
                crate::framing::AskNackReason::Backpressure,
            );
            return (
                PendingOrdinaryWrite::new(PendingOrdinaryWriteKind::AskNack { header, offset: 0 }),
                header.to_vec(),
            );
        }
        5 => {
            let mut prefix = [0; 16];
            prefix[..2].copy_from_slice(b"da");
            PendingOrdinaryWrite::new(PendingOrdinaryWriteKind::HeaderInlinePooled {
                header,
                header_len: 4,
                prefix: Some(prefix),
                prefix_len: 2,
                payload: crate::typed::PooledPayload::from_owned(
                    crate::ForwardPayload::from_static(b"ta"),
                ),
                offset: 0,
            })
        }
        _ => unreachable!(),
    };
    (pending, expected)
}

fn remaining(write: &PendingOrdinaryWrite) -> usize {
    match &write.kind {
        PendingOrdinaryWriteKind::Responses {
            frames,
            index,
            offset,
            ..
        } => {
            frames[*index..]
                .iter()
                .map(|frame| frame.wire_len())
                .sum::<usize>()
                - offset
        }
        PendingOrdinaryWriteKind::HeaderInlinePooled {
            header_len,
            prefix,
            prefix_len,
            payload,
            offset,
            ..
        } => header_len + prefix.as_ref().map_or(0, |_| *prefix_len) + payload.remaining() - offset,
        PendingOrdinaryWriteKind::HeaderInline {
            header_len,
            payload,
            header_off,
            payload_off,
            ..
        } => header_len - header_off + payload.len() - payload_off,
        PendingOrdinaryWriteKind::HeaderInlineAligned {
            header_len,
            payload,
            header_off,
            payload_off,
            ..
        } => header_len - header_off + payload.len() - payload_off,
        PendingOrdinaryWriteKind::Chunks { offset } => {
            write
                .chunks
                .as_ref()
                .expect("chunk fixture retains its common storage")
                .iter()
                .map(|c| c.len())
                .sum::<usize>()
                - offset
        }
        PendingOrdinaryWriteKind::Buf { buf } => buf.remaining(),
        PendingOrdinaryWriteKind::AskNack { header, offset } => header.len() - offset,
    }
}

#[tokio::test]
async fn all_variants_preserve_progress_across_pending_and_short_writes() {
    for vectored in [false, true] {
        for kind in 0..6 {
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
        for kind in 0..6 {
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
        for kind in 0..6 {
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
    let mut pending = PendingOrdinaryWrite::chunks(vec![bytes::Bytes::new()].into(), 0);
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
                let mut pending = PendingOrdinaryWrite::chunks(chunks.into(), 0);
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

#[test]
fn native_pooled_chunks_preserve_pointer_and_skip_metadata_promotion() {
    use crate::ask_forwarder::allocation_evidence;

    let mut owner = Vec::with_capacity(8192);
    owner.extend_from_slice(b"discardbody");
    let pointer = owner[7..].as_ptr();
    let retained = owner.capacity();
    let mut payload = crate::typed::PooledPayload::from_owned(crate::ForwardPayload::from(owner));
    payload.advance(7);
    let header = bytes::Bytes::copy_from_slice(
        &crate::framing::try_write_ask_response_header(crate::MessageType::Response, 19, 6)
            .unwrap(),
    );
    let mut writer = ScriptedWriter::new(true, []);
    let (mut chunks, counts) = allocation_evidence::measure(|| {
        chunks_for_header_pooled(
            &mut writer,
            header,
            Some(bytes::Bytes::from_static(b"pf")),
            payload,
        )
    });
    assert_eq!(counts, allocation_evidence::Counts::default());
    assert!(matches!(&chunks[2], NativeChunk::Pooled(_)));
    assert_eq!(chunks[2].as_ptr(), pointer);
    assert_eq!(chunks[2].as_ref(), b"body");
    assert_eq!(chunks[2].retained_bytes(), retained);

    let mut destination = ChunkBatch::with_capacity(6);
    let first_header =
        crate::framing::try_write_ask_response_header(crate::MessageType::Response, 18, 1).unwrap();
    destination.prepare_header(&mut writer, &first_header);
    destination
        .chunks
        .push(bytes::Bytes::copy_from_slice(&first_header).into());
    destination
        .chunks
        .push(bytes::Bytes::from_static(b"x").into());
    let (_, append_counts) = allocation_evidence::measure(|| {
        destination.append(&mut writer, &mut chunks);
    });
    assert_eq!(append_counts, allocation_evidence::Counts::default());
    assert!(!destination.invalid);
    assert!(chunks.is_empty());
    assert_eq!(destination[4].as_ptr(), pointer);
    assert_eq!(destination[4].retained_bytes(), retained);
    let pending = Some(PendingOrdinaryWrite::chunks(destination, 20));
    assert_eq!(
        pending_ordinary_write_retained_bytes(&pending),
        retained + first_header.len() + 1 + first_header.len() + 2,
    );
    let (_, positive) = allocation_evidence::measure(|| {
        std::hint::black_box(bytes::Bytes::from_owner(vec![1u8; 8]))
    });
    assert!(positive.allocations > counts.allocations);
}

#[tokio::test]
async fn native_pooled_mixed_chunks_keep_capacity_until_completion_or_cancel() {
    struct Owner {
        drops: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            b"body"
        }
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    for vectored in [false, true] {
        for cancel in [false, true] {
            let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let payload =
                crate::typed::PooledPayload::from_owned(crate::ForwardPayload::from_test_owner(
                    Owner {
                        drops: drops.clone(),
                    },
                    8192,
                ));
            let header =
                crate::framing::try_write_ask_response_header(crate::MessageType::Response, 23, 6)
                    .unwrap();
            let mut expected = header.to_vec();
            expected.extend_from_slice(b"pfbody");
            let mut writer =
                ScriptedWriter::new(vectored, [Step::Write(1), Step::Pending, Step::Write(2)]);
            let chunks = chunks_for_header_pooled(
                &mut writer,
                bytes::Bytes::copy_from_slice(&header),
                Some(bytes::Bytes::from_static(b"pf")),
                payload,
            );
            let mut pending = Some(PendingOrdinaryWrite::chunks(chunks, 0));
            let retained = 8192 + header.len() + 2;
            assert_eq!(pending_ordinary_write_retained_bytes(&pending), retained);
            assert!(matches!(
                poll_pending_ordinary_nowait(&mut writer, pending.as_mut().unwrap()).await,
                OrdinaryWriteProgress::Partial(1)
            ));
            assert_eq!(pending_ordinary_write_retained_bytes(&pending), retained);
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            if !cancel {
                for _ in 0..32 {
                    if matches!(
                        poll_pending_ordinary_nowait(&mut writer, pending.as_mut().unwrap()).await,
                        OrdinaryWriteProgress::Complete(_)
                    ) {
                        break;
                    }
                }
                assert_eq!(writer.bytes, expected, "all parts must reach the writer");
                assert!(writer.waker.is_some(), "pending path must be exercised");
                assert_eq!(drops.load(Ordering::SeqCst), 0);
            }
            release_pending_ordinary(&mut pending, &mut ResponseBatch::new(0));
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert_eq!(pending_ordinary_write_retained_bytes(&pending), 0);
        }
    }
}

#[test]
fn native_empty_pooled_view_keeps_its_full_owner_charge() {
    let mut owner = Vec::with_capacity(4096);
    owner.extend_from_slice(b"body");
    let retained = owner.capacity();
    let mut payload = crate::typed::PooledPayload::from_owned(crate::ForwardPayload::from(owner));
    payload.advance(4);
    let header =
        crate::framing::try_write_ask_response_header(crate::MessageType::Response, 24, 0).unwrap();
    let mut writer = ScriptedWriter::new(true, []);
    let chunks = chunks_for_header_pooled(
        &mut writer,
        bytes::Bytes::copy_from_slice(&header),
        None,
        payload,
    );
    assert!(matches!(&chunks[1], NativeChunk::Pooled(_)));
    assert!(chunks[1].is_empty());
    let pending = Some(PendingOrdinaryWrite::chunks(chunks, header.len()));
    assert_eq!(
        pending_ordinary_write_retained_bytes(&pending),
        header.len() + retained,
    );
}

#[test]
fn native_chunk_invalid_metadata_never_selects_opaque_fallback() {
    let mut writer = ScriptedWriter::new(true, []);
    let mut chunks = ChunkBatch::with_capacity(1);
    chunks.prepare_header(&mut writer, b"bad");
    chunks.chunks.push(bytes::Bytes::from_static(b"bad").into());
    assert!(chunks.invalid);
    assert_eq!(
        chunks.resume(&mut writer, 0).unwrap_err().kind(),
        std::io::ErrorKind::InvalidData,
    );
    assert_eq!(writer.polls, 0);
}

#[test]
fn native_pooled_partial_chunks_keep_response_budget_saturated() {
    let mut owner = Vec::with_capacity(RESPONSE_BATCH_BYTE_CAP + 1);
    owner.extend_from_slice(b"body");
    let payload = crate::typed::PooledPayload::from_owned(crate::ForwardPayload::from(owner));
    let header =
        crate::framing::try_write_ask_response_header(crate::MessageType::Response, 25, 4).unwrap();
    let mut writer = ScriptedWriter::new(true, []);
    let chunks = chunks_for_header_pooled(
        &mut writer,
        bytes::Bytes::copy_from_slice(&header),
        None,
        payload,
    );
    let mut pending = Some(PendingOrdinaryWrite::chunks(chunks, header.len() + 3));
    let mut responses = ResponseBatch::new(0);
    assert!(inline_response_budget_exhausted(&pending, &responses));
    assert_eq!(remaining(pending.as_ref().unwrap()), 1);
    release_pending_ordinary(&mut pending, &mut responses);
    assert!(!inline_response_budget_exhausted(&pending, &responses));
}

#[tokio::test]
async fn native_owned_response_batch_preserves_owner_without_metadata_allocation() {
    use crate::ask_forwarder::allocation_evidence;

    for vectored in [false, true] {
        for cancel in [false, true] {
            let mut owner = Vec::with_capacity(8192);
            owner.extend_from_slice(b"body");
            let pointer = owner.as_ptr();
            let retained = owner.capacity();
            let mut batch = ResponseBatch::new(1);
            let mut pending = None;
            let (_, counts) = allocation_evidence::measure(|| {
                batch
                    .push_owned(31, crate::ForwardPayload::from(owner))
                    .unwrap();
                assert!(park_ask_response_batch(&mut pending, &mut batch).unwrap());
            });
            assert_eq!(counts, allocation_evidence::Counts::default());
            let Some(PendingOrdinaryWrite {
                kind: PendingOrdinaryWriteKind::Responses { frames, .. },
                ..
            }) = &pending
            else {
                panic!("owned response must occupy the native response lane");
            };
            assert!(matches!(&frames[0].payload, InlineReplyPayload::Pooled(_)));
            assert_eq!(frames[0].payload.as_slice().as_ptr(), pointer);
            assert_eq!(pending_ordinary_write_retained_bytes(&pending), retained);
            let mut expected = frames[0].header.to_vec();
            expected.extend_from_slice(b"body");
            let mut writer =
                ScriptedWriter::new(vectored, [Step::Write(1), Step::Pending, Step::Write(2)]);
            assert!(matches!(
                poll_pending_ordinary_nowait(&mut writer, pending.as_mut().unwrap()).await,
                OrdinaryWriteProgress::Partial(1)
            ));
            assert_eq!(pending_ordinary_write_retained_bytes(&pending), retained);
            if !cancel {
                for _ in 0..32 {
                    if matches!(
                        poll_pending_ordinary_nowait(&mut writer, pending.as_mut().unwrap()).await,
                        OrdinaryWriteProgress::Complete(_)
                    ) {
                        break;
                    }
                }
                assert_eq!(writer.bytes, expected);
                assert!(writer.waker.is_some());
            }
            release_pending_ordinary(&mut pending, &mut batch);
            assert_eq!(pending_ordinary_write_retained_bytes(&pending), 0);
            assert!(batch.spare.is_empty());
            assert!(
                batch.spare.capacity() >= 1,
                "response storage remains reusable"
            );
        }
    }
}
