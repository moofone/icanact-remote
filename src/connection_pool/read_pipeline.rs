/// R-6: one-shot handoff of the accept path's first-frame `StreamingState` to
/// this connection's IO task. See `ReadContext::streaming_state_handoff`.
pub(crate) struct StreamingStateHandoff {
    pub(crate) cell: std::sync::Mutex<Option<crate::protocol::StreamingState>>,
    pub(crate) ready: tokio::sync::Notify,
}

#[derive(Clone)]
#[doc(hidden)]
pub struct ReadContext {
    pub(crate) registry_weak: std::sync::Weak<GossipRegistry>,
    pub(crate) peer_addr: SocketAddr,
    /// R-11: this exact connection's own session discriminator, unique per
    /// physical connection.
    ///
    /// For inbound connections this equals `peer_addr` (the remote client's
    /// ephemeral source port, already unique per connection). For outbound
    /// connections it is THIS socket's own local ephemeral port, not the
    /// dial target -- the dial target (`peer_addr` for an outbound
    /// connection) is the peer's fixed listening port and is identical for
    /// every connection we ever make to it, so it cannot distinguish a
    /// redial's new connection from an old one still draining. Threaded
    /// through to `merge_full_sync_from` so the R-11 restart-sequence
    /// exemption can only ever be armed or consumed by the connection that
    /// established it.
    pub(crate) session_source: SocketAddr,
    /// Best-effort peer identity for this connection.
    ///
    /// This is used to avoid mis-attributing disconnects from stale/duplicate
    /// connections (for example tie-breaker drops during simultaneous dial).
    pub(crate) peer_id: Option<crate::PeerId>,
    pub(crate) max_message_size: usize,
    pub(crate) expected_schema_hash: Option<u64>,
    pub(crate) aligned_pool: Arc<crate::AlignedBytesPool>,
    /// Route bindings are scoped to this exact transport connection.
    pub(crate) inbound_routes: Arc<crate::route_interning::RouteTable>,
    pub(crate) response_correlation: Option<Arc<CorrelationTracker>>,
    pub(crate) response_writer: Option<Arc<crate::ask_responder::ResponseWriter>>,
    pub(crate) tell_handler_sync: Option<Arc<crate::registry::ActorTellHandlerSyncCell>>,
    pub(crate) tell_handler_sync_context:
        Option<Arc<crate::registry::ActorTellHandlerSyncContextCell>>,
    pub(crate) ask_immediate_handler_sync:
        Option<Arc<crate::registry::ActorAskImmediateHandlerSyncCell>>,
    pub(crate) ask_handler_sync: Option<Arc<crate::registry::ActorAskHandlerSyncCell>>,
    pub(crate) sync_actor_handler: Option<Arc<crate::registry::ActorMessageHandlerSyncCell>>,
    /// R-6: one-shot handoff of the accept path's first-frame `StreamingState`
    /// to this connection's IO task, so a multi-chunk `StreamStart` arriving as
    /// the connection's first frame is not split across two separate states
    /// (which would tear the connection down on the follow-up chunk). The IO
    /// task awaits `ready` and takes the state before its read loop; the accept
    /// path fills `cell` and notifies once it has processed the first frame.
    /// `None` on outbound connections and in tests (no separate first-frame
    /// read).
    pub(crate) streaming_state_handoff: Option<Arc<StreamingStateHandoff>>,
}

#[cfg(test)]
mod read_pipeline_tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use tokio::io::AsyncWriteExt;

    /// The direct-response stream-id allocator is process-global; serialize the
    /// tests that swap/observe it so they cannot race each other under parallel
    /// test execution.
    fn allocator_test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[tokio::test]
    async fn zero_length_frame_is_rejected_before_body_read() {
        let (mut writer, mut reader) = tokio::io::duplex(crate::framing::LENGTH_PREFIX_LEN);
        writer.write_all(&0u32.to_be_bytes()).await.unwrap();

        let ctx = super::ReadContext {
            streaming_state_handoff: None,
            registry_weak: std::sync::Weak::new(),
            peer_addr: "127.0.0.1:9000".parse().unwrap(),
            session_source: "127.0.0.1:9000".parse().unwrap(),
            peer_id: None,
            max_message_size: 1024,
            expected_schema_hash: None,
            aligned_pool: Arc::new(crate::AlignedBytesPool::default()),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
            response_correlation: None,
            response_writer: None,
            tell_handler_sync: None,
            tell_handler_sync_context: None,
            ask_immediate_handler_sync: None,
            ask_handler_sync: None,
            sync_actor_handler: None,
        };

        let error = super::read_message_step(
            &mut reader,
            &mut super::ReadState::new(),
            &ctx,
            &mut crate::protocol::StreamingState::new(),
        )
            .await
            .expect_err("zero-length frames must be rejected before entering ReadBody");
        assert!(matches!(error, crate::GossipError::Network(ref io) if io.kind() == std::io::ErrorKind::InvalidData));
    }

    #[tokio::test]
    async fn stream_abort_uses_complete_frame_path_without_reservation() {
        let frame = crate::framing::write_stream_abort_header(7, 9);
        let (mut writer, mut reader) = tokio::io::duplex(frame.len());
        writer.write_all(&frame).await.unwrap();
        let ctx = super::ReadContext {
            streaming_state_handoff: None,
            registry_weak: std::sync::Weak::new(), peer_addr: "127.0.0.1:9001".parse().unwrap(),
            session_source: "127.0.0.1:9001".parse().unwrap(),
            peer_id: None, max_message_size: 1024, expected_schema_hash: None,
            aligned_pool: Arc::new(crate::AlignedBytesPool::default()),
            inbound_routes: Arc::new(crate::route_interning::RouteTable::new()), response_correlation: None,
            response_writer: None, tell_handler_sync: None, tell_handler_sync_context: None,
            ask_immediate_handler_sync: None, ask_handler_sync: None, sync_actor_handler: None,
        };
        let mut state = super::ReadState::new();
        let mut streams = crate::protocol::StreamingState::new();
        let _ = super::read_message_step(&mut reader, &mut state, &ctx, &mut streams)
            .await.unwrap();
        let result = super::read_message_step(&mut reader, &mut state, &ctx, &mut streams)
            .await.unwrap().expect("StreamAbort must produce a result");
        assert!(matches!(result,
            crate::handle::MessageReadResult::StreamAbort { stream_id: 7, reason: 9 }
        ));
    }

    #[test]
    fn direct_response_stream_ids_restart_after_wrap() {
        let _g = allocator_test_lock();
        // R-5: even partition — the max even id (u32::MAX - 1) wraps back to 2,
        // never colliding with the per-handle allocator's odd ids.
        let previous =
            super::NEXT_DIRECT_RESPONSE_STREAM_ID.swap(u32::MAX - 1, Ordering::SeqCst);
        assert_eq!(super::allocate_direct_response_stream_id().unwrap(), u32::MAX - 1);
        assert_eq!(super::allocate_direct_response_stream_id().unwrap(), 2);
        super::NEXT_DIRECT_RESPONSE_STREAM_ID.store(previous, Ordering::SeqCst);
    }

    /// R-5: direct-response stream ids occupy the even partition (disjoint from
    /// the per-handle stream allocator's odd ids), so they can never collide on
    /// `stream_id` with a handle-initiated stream on the same connection.
    #[test]
    fn qa_r5_direct_response_stream_ids_are_even() {
        let _g = allocator_test_lock();
        let previous = super::NEXT_DIRECT_RESPONSE_STREAM_ID.swap(2, Ordering::SeqCst);
        let mut ids = std::collections::HashSet::new();
        for _ in 0..10_000 {
            let id = super::allocate_direct_response_stream_id().unwrap();
            assert!(
                id != 0 && id % 2 == 0,
                "direct-response stream id must be even and nonzero, got {id}"
            );
            assert!(ids.insert(id), "direct-response stream id reused: {id}");
        }
        super::NEXT_DIRECT_RESPONSE_STREAM_ID.store(previous, Ordering::SeqCst);
    }
}

enum ReadState {
    ReadLen {
        buf: [u8; crate::framing::LENGTH_PREFIX_LEN],
        read: usize,
    },
    ReadBody {
        msg_len: usize,
        buffer: crate::PooledAlignedBuffer,
        read: usize,
    },
    /// V5 stream metadata is read into a small fixed stack buffer before a
    /// final assembly range is reserved. This avoids a temporary chunk-sized
    /// frame allocation.
    ReadStreamMeta {
        kind: crate::framing::WireKind,
        body_len: usize,
        meta: [u8; crate::framing::STREAM_REQUEST_START_HEADER_LEN],
        meta_len: usize,
        read: usize,
    },
    /// The TLS reader writes directly into the reservation held by
    /// `StreamingState`; the bitmap is committed only at frame completion.
    ReadStreamPayload {
        reservation: crate::protocol::StreamChunkReservation,
        read: usize,
    },
    /// Payload of a stream rejected for bounded resource pressure. Consume it
    /// into a fixed scratch buffer to keep framing aligned without allocating.
    DiscardStreamPayload {
        remaining: usize,
        scratch: Box<[u8; 8192]>,
    },
}

// Direct responses are emitted by the IO owner rather than a public
// `LockFreeStreamHandle` method. R-5: request and response streams share one
// reassembly map keyed by `stream_id` alone (no direction bit), so this
// counter takes the EVEN partition (2, 4, 6, ...) — disjoint from each
// per-handle `LockFreeStreamHandle` allocator's ODD ids — and a direct
// streaming response can never collide with a handle-initiated stream on the
// same connection (a collision keys two live streams to one id and tears the
// connection down as a duplicate start). Reserve zero; wrap the max even id
// back to 2 instead of poisoning every connection.
static NEXT_DIRECT_RESPONSE_STREAM_ID: AtomicU32 = AtomicU32::new(2);

fn allocate_direct_response_stream_id() -> Result<u32> {
    loop {
        let id = NEXT_DIRECT_RESPONSE_STREAM_ID.load(Ordering::Relaxed);
        // Even partition (R-5): step 2, wrapping the max even id (u32::MAX - 1)
        // back to 2. `id` is always even and nonzero, so the odd half stays free
        // for the per-handle stream allocator.
        let next = if id >= u32::MAX - 1 { 2 } else { id + 2 };
        if NEXT_DIRECT_RESPONSE_STREAM_ID
            .compare_exchange_weak(id, next, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return Ok(id);
        }
    }
}

fn stream_meta_len(kind: crate::framing::WireKind) -> Option<usize> {
    match kind {
        crate::framing::WireKind::StreamStart => Some(crate::framing::STREAM_REQUEST_START_HEADER_LEN),
        crate::framing::WireKind::StreamResponseStart => Some(crate::framing::STREAM_RESPONSE_START_HEADER_LEN),
        crate::framing::WireKind::StreamData | crate::framing::WireKind::StreamResponseData => {
            Some(crate::framing::STREAM_DATA_HEADER_LEN)
        }
        _ => None,
    }
}

fn completed_v5_stream_result(completed: crate::protocol::CompletedV5Stream) -> crate::handle::MessageReadResult {
    if completed.is_response {
        crate::handle::MessageReadResult::Response {
            correlation_id: completed.correlation_id,
            payload: completed.payload,
        }
    } else {
        crate::handle::MessageReadResult::Actor {
            msg_type: if completed.correlation_id == 0 {
                crate::MessageType::ActorTell as u8
            } else {
                crate::MessageType::ActorAsk as u8
            },
            correlation_id: completed.correlation_id,
            actor_id: completed.actor_id,
            type_hash: completed.type_hash,
            schema_hash: None,
            payload: completed.payload,
        }
    }
}

fn reserve_v5_stream_payload(
    kind: crate::framing::WireKind,
    body_len: usize,
    meta: &[u8],
    streaming_state: &mut crate::protocol::StreamingState,
    pool: Arc<crate::AlignedBytesPool>,
) -> Result<Option<crate::protocol::StreamChunkReservation>> {
    let meta_len = stream_meta_len(kind).expect("stream kind has metadata");
    let payload_len = body_len.checked_sub(meta_len).ok_or_else(|| GossipError::Network(
        std::io::Error::new(std::io::ErrorKind::InvalidData, "truncated V5 stream metadata"),
    ))?;
    match kind {
        crate::framing::WireKind::StreamStart => {
            let stream_id = u32::from_be_bytes(meta[..4].try_into().unwrap()) as u64;
            let correlation_id = u32::from_be_bytes(meta[4..8].try_into().unwrap());
            let total_size = u32::from_be_bytes(meta[8..12].try_into().unwrap()) as u64;
            let actor_id = u64::from_be_bytes(meta[12..20].try_into().unwrap());
            let type_hash = u32::from_be_bytes(meta[20..24].try_into().unwrap());
            streaming_state.begin_v5_stream_or_discard(
                crate::StreamHeader { stream_id, total_size, chunk_size: payload_len as u32, chunk_index: 0, type_hash, actor_id },
                correlation_id,
                pool,
                false,
                payload_len,
            )
        }
        crate::framing::WireKind::StreamResponseStart => {
            let stream_id = u32::from_be_bytes(meta[..4].try_into().unwrap()) as u64;
            let correlation_id = u32::from_be_bytes(meta[4..8].try_into().unwrap());
            let total_size = u32::from_be_bytes(meta[8..12].try_into().unwrap()) as u64;
            streaming_state.begin_v5_stream_or_discard(
                crate::StreamHeader { stream_id, total_size, chunk_size: payload_len as u32, chunk_index: 0, type_hash: 0, actor_id: 0 },
                correlation_id,
                pool,
                true,
                payload_len,
            )
        }
        crate::framing::WireKind::StreamData | crate::framing::WireKind::StreamResponseData => {
            let stream_id = u32::from_be_bytes(meta[..4].try_into().unwrap()) as u64;
            let chunk_index = u32::from_be_bytes(meta[4..8].try_into().unwrap());
            streaming_state.reserve_v5_chunk_or_discard(stream_id, chunk_index, payload_len)
        }
        _ => unreachable!("non-stream kind passed to direct stream reservation"),
    }
}

fn stream_payload_state(
    reservation: Option<crate::protocol::StreamChunkReservation>,
    body_len: usize,
    meta_len: usize,
) -> ReadState {
    match reservation {
        Some(reservation) if reservation.is_empty() => ReadState::new(),
        Some(reservation) => ReadState::ReadStreamPayload { reservation, read: 0 },
        None => ReadState::DiscardStreamPayload {
            remaining: body_len - meta_len,
            scratch: Box::new([0; 8192]),
        },
    }
}

#[inline]
fn reject_zero_length_frame() -> GossipError {
    GossipError::Network(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "zero-length frame is invalid",
    ))
}

impl ReadState {
    fn new() -> Self {
        Self::ReadLen {
            buf: [0u8; crate::framing::LENGTH_PREFIX_LEN],
            read: 0,
        }
    }
}

#[allow(dead_code)]
async fn read_message_step<S>(
    stream: &mut S,
    state: &mut ReadState,
    ctx: &ReadContext,
    streaming_state: &mut crate::protocol::StreamingState,
) -> Result<Option<crate::handle::MessageReadResult>>
where
    S: AsyncRead + Unpin,
{
    match state {
        ReadState::ReadLen { buf, read } => {
            let n = stream.read(&mut buf[*read..]).await?;
            if n == 0 {
                return Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed",
                )));
            }
            *read += n;
            if *read < crate::framing::LENGTH_PREFIX_LEN {
                return Ok(None);
            }

            let control = crate::framing::decode_control(*buf)
                .ok_or_else(|| crate::GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "unknown V5 wire kind",
                )))?;
            let msg_len = control.body_len;
            if msg_len == 0 {
                return Err(reject_zero_length_frame());
            }
            if msg_len > ctx.max_message_size {
                return Err(GossipError::MessageTooLarge {
                    size: msg_len,
                    max: ctx.max_message_size,
                });
            }

            if let Some(meta_len) = stream_meta_len(control.kind) {
                if msg_len < meta_len {
                    return Err(GossipError::Network(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "truncated V5 stream metadata",
                    )));
                }
                *state = ReadState::ReadStreamMeta {
                    kind: control.kind,
                    body_len: msg_len,
                    meta: [0; crate::framing::STREAM_REQUEST_START_HEADER_LEN],
                    meta_len,
                    read: 0,
                };
                return Ok(None);
            }

            let total_len = msg_len + crate::framing::LENGTH_PREFIX_LEN;
            let mut buffer =
                crate::PooledAlignedBuffer::with_len(total_len, ctx.aligned_pool.clone());
            buffer.as_mut_slice()[..crate::framing::LENGTH_PREFIX_LEN].copy_from_slice(buf);

            *state = ReadState::ReadBody {
                msg_len,
                buffer,
                read: 0,
            };
            Ok(None)
        }
        ReadState::ReadBody {
            msg_len,
            buffer,
            read,
        } => {
            let offset = crate::framing::LENGTH_PREFIX_LEN + *read;
            let end = crate::framing::LENGTH_PREFIX_LEN + *msg_len;
            let n = stream.read(&mut buffer.as_mut_slice()[offset..end]).await?;
            if n == 0 {
                return Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed",
                )));
            }
            *read += n;
            if *read < *msg_len {
                return Ok(None);
            }

            let (msg_len, buffer) = match std::mem::replace(state, ReadState::new()) {
                ReadState::ReadBody {
                    msg_len, buffer, ..
                } => (msg_len, buffer),
                _ => unreachable!("read state must be ReadBody when complete"),
            };

            let result = crate::handle::parse_message_from_pooled_buffer_with_routes(
                buffer,
                msg_len,
                Some(&ctx.inbound_routes),
            )?;
            Ok(Some(result))
        }
        ReadState::ReadStreamMeta { kind, body_len, meta, meta_len, read } => {
            let n = stream.read(&mut meta[*read..*meta_len]).await?;
            if n == 0 {
                return Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed during V5 stream metadata",
                )));
            }
            *read += n;
            if *read < *meta_len {
                return Ok(None);
            }
            let reservation = reserve_v5_stream_payload(
                *kind, *body_len, &meta[..*meta_len], streaming_state, ctx.aligned_pool.clone(),
            )?;
            *state = stream_payload_state(reservation, *body_len, *meta_len);
            Ok(None)
        }
        ReadState::ReadStreamPayload { reservation, read } => {
            let target = streaming_state.v5_chunk_target(*reservation, *read)?;
            let n = stream.read(target).await?;
            if n == 0 {
                return Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed during V5 stream payload",
                )));
            }
            *read += n;
            if *read < reservation.len() {
                return Ok(None);
            }
            let reservation = match std::mem::replace(state, ReadState::new()) {
                ReadState::ReadStreamPayload { reservation, .. } => reservation,
                _ => unreachable!("read state must be V5 stream payload"),
            };
            Ok(streaming_state.commit_v5_chunk(reservation)?.map(completed_v5_stream_result))
        }
        ReadState::DiscardStreamPayload { remaining, scratch } => {
            let read_len = (*remaining).min(scratch.len());
            if read_len == 0 {
                *state = ReadState::new();
                return Ok(None);
            }
            let n = stream.read(&mut scratch[..read_len]).await?;
            if n == 0 {
                return Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed while discarding rejected stream payload",
                )));
            }
            *remaining -= n;
            if *remaining == 0 {
                *state = ReadState::new();
            }
            Ok(None)
        }
    }
}

struct ReadPollResult {
    result: Option<ReadIoResult>,
    progressed: bool,
}

#[expect(
    dead_code,
    reason = "the direct ask fast-path shares these result variants with the IO owner and remains intentionally pre-wired"
)]
enum ReadIoResult {
    Generic(crate::handle::MessageReadResult),
    DirectAsk {
        correlation_id: u32,
        payload: crate::AlignedBytes,
    },
    ActorAsk {
        correlation_id: u32,
        actor_id: u64,
        type_hash: u32,
        payload: crate::AlignedBytes,
    },
}

#[expect(
    dead_code,
    reason = "the direct ask fast-path result variants are retained for the zero-copy parser seam"
)]
enum FastReadOutcome {
    Handled,
    Parsed(ReadIoResult),
    Unhandled(crate::PooledAlignedBuffer),
}

fn try_handle_read_fast_from_pooled(
    buffer: crate::PooledAlignedBuffer,
    _msg_len: usize,
    _ctx: &ReadContext,
) -> Result<FastReadOutcome> {
    // V5's shared parser owns the packed-control dispatch. Keeping this seam
    // intentionally boring until the specialized direct-read stream state lands
    // avoids duplicating V5 frame decoding in the I/O hot path.
    Ok(FastReadOutcome::Unhandled(buffer))
}

fn poll_read_once<S>(stream: &mut S, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>>
where
    S: AsyncRead + Unpin,
{
    let mut read_buf = ReadBuf::new(buf);
    match Pin::new(stream).poll_read(cx, &mut read_buf) {
        Poll::Pending => Poll::Pending,
        Poll::Ready(Ok(())) => Poll::Ready(Ok(read_buf.filled().len())),
        Poll::Ready(Err(e)) => Poll::Ready(Err(GossipError::Network(e))),
    }
}

async fn read_message_step_poll<S>(
    stream: &mut S,
    state: &mut ReadState,
    ctx: &ReadContext,
    streaming_state: &mut crate::protocol::StreamingState,
    block_on_pending: bool,
) -> Result<ReadPollResult>
where
    S: AsyncRead + Unpin,
{
    futures::future::poll_fn(|cx| match state {
        ReadState::ReadLen { buf, read } => {
            let target = &mut buf[*read..];
            if target.is_empty() {
                return Poll::Ready(Ok(ReadPollResult {
                    result: None,
                    progressed: false,
                }));
            }
            match poll_read_once(stream, cx, target) {
                Poll::Pending => {
                    if block_on_pending {
                        Poll::Pending
                    } else {
                        Poll::Ready(Ok(ReadPollResult {
                            result: None,
                            progressed: false,
                        }))
                    }
                }
                Poll::Ready(Ok(0)) => Poll::Ready(Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed",
                )))),
                Poll::Ready(Ok(n)) => {
                    *read += n;
                    if *read < crate::framing::LENGTH_PREFIX_LEN {
                        return Poll::Ready(Ok(ReadPollResult {
                            result: None,
                            progressed: true,
                        }));
                    }

                    let control = crate::framing::decode_control(*buf)
                        .ok_or_else(|| crate::GossipError::Network(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "unknown V5 wire kind",
                        )))?;
                    let msg_len = control.body_len;
                    if msg_len == 0 {
                        return Poll::Ready(Err(reject_zero_length_frame()));
                    }
                    if msg_len > ctx.max_message_size {
                        return Poll::Ready(Err(GossipError::MessageTooLarge {
                            size: msg_len,
                            max: ctx.max_message_size,
                        }));
                    }

                    if let Some(meta_len) = stream_meta_len(control.kind) {
                        if msg_len < meta_len {
                            return Poll::Ready(Err(GossipError::Network(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "truncated V5 stream metadata",
                            ))));
                        }
                        *state = ReadState::ReadStreamMeta {
                            kind: control.kind,
                            body_len: msg_len,
                            meta: [0; crate::framing::STREAM_REQUEST_START_HEADER_LEN],
                            meta_len,
                            read: 0,
                        };
                        return Poll::Ready(Ok(ReadPollResult { result: None, progressed: true }));
                    }
                    let total_len = msg_len + crate::framing::LENGTH_PREFIX_LEN;
                    let mut buffer = unsafe {
                        crate::PooledAlignedBuffer::with_len_uninit(
                            total_len,
                            ctx.aligned_pool.clone(),
                        )
                    };
                    buffer.as_mut_slice()[..crate::framing::LENGTH_PREFIX_LEN].copy_from_slice(buf);

                    *state = ReadState::ReadBody {
                        msg_len,
                        buffer,
                        read: 0,
                    };
                    Poll::Ready(Ok(ReadPollResult {
                        result: None,
                        progressed: true,
                    }))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            }
        }
        ReadState::ReadBody {
            msg_len,
            buffer,
            read,
        } => {
            let offset = crate::framing::LENGTH_PREFIX_LEN + *read;
            let end = crate::framing::LENGTH_PREFIX_LEN + *msg_len;
            let target = &mut buffer.as_mut_slice()[offset..end];
            if target.is_empty() {
                return Poll::Ready(Ok(ReadPollResult {
                    result: None,
                    progressed: false,
                }));
            }
            match poll_read_once(stream, cx, target) {
                Poll::Pending => {
                    if block_on_pending {
                        Poll::Pending
                    } else {
                        Poll::Ready(Ok(ReadPollResult {
                            result: None,
                            progressed: false,
                        }))
                    }
                }
                Poll::Ready(Ok(0)) => Poll::Ready(Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed",
                )))),
                Poll::Ready(Ok(n)) => {
                    *read += n;
                    if *read < *msg_len {
                        return Poll::Ready(Ok(ReadPollResult {
                            result: None,
                            progressed: true,
                        }));
                    }

                    let (msg_len, buffer) = match std::mem::replace(state, ReadState::new()) {
                        ReadState::ReadBody {
                            msg_len, buffer, ..
                        } => (msg_len, buffer),
                        _ => unreachable!("read state must be ReadBody when complete"),
                    };

                    let result = match try_handle_read_fast_from_pooled(buffer, msg_len, ctx)? {
                        FastReadOutcome::Handled => {
                            return Poll::Ready(Ok(ReadPollResult {
                                result: None,
                                progressed: true,
                            }));
                        }
                        FastReadOutcome::Parsed(result) => result,
                        FastReadOutcome::Unhandled(buffer) => ReadIoResult::Generic(
                            crate::handle::parse_message_from_pooled_buffer_with_routes(
                                buffer,
                                msg_len,
                                Some(&ctx.inbound_routes),
                            )?,
                        ),
                    };
                    Poll::Ready(Ok(ReadPollResult {
                        result: Some(result),
                        progressed: true,
                    }))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            }
        }
        ReadState::ReadStreamMeta { kind, body_len, meta, meta_len, read } => {
            match poll_read_once(stream, cx, &mut meta[*read..*meta_len]) {
                Poll::Pending if block_on_pending => Poll::Pending,
                Poll::Pending => Poll::Ready(Ok(ReadPollResult { result: None, progressed: false })),
                Poll::Ready(Ok(0)) => Poll::Ready(Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof, "connection closed during V5 stream metadata",
                )))),
                Poll::Ready(Ok(n)) => {
                    *read += n;
                    if *read < *meta_len {
                        return Poll::Ready(Ok(ReadPollResult { result: None, progressed: true }));
                    }
                    let reservation = reserve_v5_stream_payload(
                        *kind, *body_len, &meta[..*meta_len], streaming_state, ctx.aligned_pool.clone(),
                    )?;
                    *state = stream_payload_state(reservation, *body_len, *meta_len);
                    Poll::Ready(Ok(ReadPollResult { result: None, progressed: true }))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            }
        }
        ReadState::ReadStreamPayload { reservation, read } => {
            let target = streaming_state.v5_chunk_target(*reservation, *read)?;
            match poll_read_once(stream, cx, target) {
                Poll::Pending if block_on_pending => Poll::Pending,
                Poll::Pending => Poll::Ready(Ok(ReadPollResult { result: None, progressed: false })),
                Poll::Ready(Ok(0)) => Poll::Ready(Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof, "connection closed during V5 stream payload",
                )))),
                Poll::Ready(Ok(n)) => {
                    *read += n;
                    if *read < reservation.len() {
                        return Poll::Ready(Ok(ReadPollResult { result: None, progressed: true }));
                    }
                    let reservation = match std::mem::replace(state, ReadState::new()) {
                        ReadState::ReadStreamPayload { reservation, .. } => reservation,
                        _ => unreachable!(),
                    };
                    let result = streaming_state.commit_v5_chunk(reservation)?
                        .map(completed_v5_stream_result)
                        .map(ReadIoResult::Generic);
                    Poll::Ready(Ok(ReadPollResult { result, progressed: true }))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            }
        }
        ReadState::DiscardStreamPayload { remaining, scratch } => {
            let read_len = (*remaining).min(scratch.len());
            if read_len == 0 {
                *state = ReadState::new();
                return Poll::Ready(Ok(ReadPollResult { result: None, progressed: true }));
            }
            match poll_read_once(stream, cx, &mut scratch[..read_len]) {
                Poll::Pending if block_on_pending => Poll::Pending,
                Poll::Pending => Poll::Ready(Ok(ReadPollResult { result: None, progressed: false })),
                Poll::Ready(Ok(0)) => Poll::Ready(Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed while discarding rejected stream payload",
                )))),
                Poll::Ready(Ok(n)) => {
                    *remaining -= n;
                    if *remaining == 0 {
                        *state = ReadState::new();
                    }
                    Poll::Ready(Ok(ReadPollResult { result: None, progressed: true }))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            }
        }
    })
    .await
}

/// Non-blocking variant of `read_message_step_poll`.
///
/// This is used in the IO task "did_work" batch path to avoid a subtle deadlock:
/// awaiting a `poll_read` future will park the entire IO task until the socket is readable,
/// which prevents draining the write queue for newly enqueued asks/tells.
///
/// Semantics: if the socket is not currently readable, returns `progressed=false` immediately.
async fn read_message_step_nonblocking<S>(
    stream: &mut S,
    state: &mut ReadState,
    ctx: &ReadContext,
    streaming_state: &mut crate::protocol::StreamingState,
) -> Result<ReadPollResult>
where
    S: AsyncRead + Unpin,
{
    futures::future::poll_fn(|cx| match state {
        ReadState::ReadLen { buf, read } => {
            let target = &mut buf[*read..];
            if target.is_empty() {
                return Poll::Ready(Ok(ReadPollResult {
                    result: None,
                    progressed: false,
                }));
            }
            match poll_read_once(stream, cx, target) {
                Poll::Pending => Poll::Ready(Ok(ReadPollResult {
                    result: None,
                    progressed: false,
                })),
                Poll::Ready(Ok(0)) => Poll::Ready(Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed",
                )))),
                Poll::Ready(Ok(n)) => {
                    *read += n;
                    if *read < crate::framing::LENGTH_PREFIX_LEN {
                        return Poll::Ready(Ok(ReadPollResult {
                            result: None,
                            progressed: true,
                        }));
                    }

                    let control = crate::framing::decode_control(*buf)
                        .ok_or_else(|| crate::GossipError::Network(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "unknown V5 wire kind",
                        )))?;
                    let msg_len = control.body_len;
                    if msg_len == 0 {
                        return Poll::Ready(Err(reject_zero_length_frame()));
                    }
                    if msg_len > ctx.max_message_size {
                        return Poll::Ready(Err(GossipError::MessageTooLarge {
                            size: msg_len,
                            max: ctx.max_message_size,
                        }));
                    }

                    if let Some(meta_len) = stream_meta_len(control.kind) {
                        if msg_len < meta_len {
                            return Poll::Ready(Err(GossipError::Network(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "truncated V5 stream metadata",
                            ))));
                        }
                        *state = ReadState::ReadStreamMeta {
                            kind: control.kind,
                            body_len: msg_len,
                            meta: [0; crate::framing::STREAM_REQUEST_START_HEADER_LEN],
                            meta_len,
                            read: 0,
                        };
                        return Poll::Ready(Ok(ReadPollResult { result: None, progressed: true }));
                    }
                    let total_len = msg_len + crate::framing::LENGTH_PREFIX_LEN;
                    let mut buffer = unsafe {
                        crate::PooledAlignedBuffer::with_len_uninit(
                            total_len,
                            ctx.aligned_pool.clone(),
                        )
                    };
                    buffer.as_mut_slice()[..crate::framing::LENGTH_PREFIX_LEN].copy_from_slice(buf);

                    *state = ReadState::ReadBody {
                        msg_len,
                        buffer,
                        read: 0,
                    };
                    Poll::Ready(Ok(ReadPollResult {
                        result: None,
                        progressed: true,
                    }))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            }
        }
        ReadState::ReadBody {
            msg_len,
            buffer,
            read,
        } => {
            let offset = crate::framing::LENGTH_PREFIX_LEN + *read;
            let end = crate::framing::LENGTH_PREFIX_LEN + *msg_len;
            let target = &mut buffer.as_mut_slice()[offset..end];
            if target.is_empty() {
                return Poll::Ready(Ok(ReadPollResult {
                    result: None,
                    progressed: false,
                }));
            }
            match poll_read_once(stream, cx, target) {
                Poll::Pending => Poll::Ready(Ok(ReadPollResult {
                    result: None,
                    progressed: false,
                })),
                Poll::Ready(Ok(0)) => Poll::Ready(Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed",
                )))),
                Poll::Ready(Ok(n)) => {
                    *read += n;
                    if *read < *msg_len {
                        return Poll::Ready(Ok(ReadPollResult {
                            result: None,
                            progressed: true,
                        }));
                    }

                    let (msg_len, buffer) = match std::mem::replace(state, ReadState::new()) {
                        ReadState::ReadBody {
                            msg_len, buffer, ..
                        } => (msg_len, buffer),
                        _ => unreachable!("read state must be ReadBody when complete"),
                    };

                    let result = match try_handle_read_fast_from_pooled(buffer, msg_len, ctx)? {
                        FastReadOutcome::Handled => {
                            return Poll::Ready(Ok(ReadPollResult {
                                result: None,
                                progressed: true,
                            }));
                        }
                        FastReadOutcome::Parsed(result) => result,
                        FastReadOutcome::Unhandled(buffer) => ReadIoResult::Generic(
                            crate::handle::parse_message_from_pooled_buffer_with_routes(
                                buffer,
                                msg_len,
                                Some(&ctx.inbound_routes),
                            )?,
                        ),
                    };
                    Poll::Ready(Ok(ReadPollResult {
                        result: Some(result),
                        progressed: true,
                    }))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            }
        }
        ReadState::ReadStreamMeta { kind, body_len, meta, meta_len, read } => {
            match poll_read_once(stream, cx, &mut meta[*read..*meta_len]) {
                Poll::Pending => Poll::Ready(Ok(ReadPollResult { result: None, progressed: false })),
                Poll::Ready(Ok(0)) => Poll::Ready(Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof, "connection closed during V5 stream metadata",
                )))),
                Poll::Ready(Ok(n)) => {
                    *read += n;
                    if *read < *meta_len {
                        return Poll::Ready(Ok(ReadPollResult { result: None, progressed: true }));
                    }
                    let reservation = reserve_v5_stream_payload(
                        *kind, *body_len, &meta[..*meta_len], streaming_state, ctx.aligned_pool.clone(),
                    )?;
                    *state = stream_payload_state(reservation, *body_len, *meta_len);
                    Poll::Ready(Ok(ReadPollResult { result: None, progressed: true }))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            }
        }
        ReadState::ReadStreamPayload { reservation, read } => {
            let target = streaming_state.v5_chunk_target(*reservation, *read)?;
            match poll_read_once(stream, cx, target) {
                Poll::Pending => Poll::Ready(Ok(ReadPollResult { result: None, progressed: false })),
                Poll::Ready(Ok(0)) => Poll::Ready(Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof, "connection closed during V5 stream payload",
                )))),
                Poll::Ready(Ok(n)) => {
                    *read += n;
                    if *read < reservation.len() {
                        return Poll::Ready(Ok(ReadPollResult { result: None, progressed: true }));
                    }
                    let reservation = match std::mem::replace(state, ReadState::new()) {
                        ReadState::ReadStreamPayload { reservation, .. } => reservation,
                        _ => unreachable!(),
                    };
                    let result = streaming_state.commit_v5_chunk(reservation)?
                        .map(completed_v5_stream_result)
                        .map(ReadIoResult::Generic);
                    Poll::Ready(Ok(ReadPollResult { result, progressed: true }))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            }
        }
        ReadState::DiscardStreamPayload { remaining, scratch } => {
            let read_len = (*remaining).min(scratch.len());
            if read_len == 0 {
                *state = ReadState::new();
                return Poll::Ready(Ok(ReadPollResult { result: None, progressed: true }));
            }
            match poll_read_once(stream, cx, &mut scratch[..read_len]) {
                Poll::Pending => Poll::Ready(Ok(ReadPollResult { result: None, progressed: false })),
                Poll::Ready(Ok(0)) => Poll::Ready(Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed while discarding rejected stream payload",
                )))),
                Poll::Ready(Ok(n)) => {
                    *remaining -= n;
                    if *remaining == 0 {
                        *state = ReadState::new();
                    }
                    Poll::Ready(Ok(ReadPollResult { result: None, progressed: true }))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            }
        }
    })
    .await
}

async fn write_header_payload_vectored<S>(
    stream: &mut S,
    bytes_written_counter: &Arc<AtomicUsize>,
    bytes_since_flush: &mut usize,
    header: &[u8],
    payload: &[u8],
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    if payload.is_empty() {
        stream
            .write_all(header)
            .await
            .map_err(GossipError::Network)?;
        bytes_written_counter.fetch_add(header.len(), Ordering::Relaxed);
        *bytes_since_flush += header.len();
        return Ok(());
    }

    let slices = [
        std::io::IoSlice::new(header),
        std::io::IoSlice::new(payload),
    ];
    match stream.write_vectored(&slices).await {
        Ok(n) if n == header.len() + payload.len() => {
            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
            *bytes_since_flush += n;
            Ok(())
        }
        Ok(n) => {
            bytes_written_counter.fetch_add(n, Ordering::Relaxed);
            *bytes_since_flush += n;
            if n < header.len() {
                stream
                    .write_all(&header[n..])
                    .await
                    .map_err(GossipError::Network)?;
                bytes_written_counter.fetch_add(header.len() - n, Ordering::Relaxed);
                *bytes_since_flush += header.len() - n;
                if !payload.is_empty() {
                    stream
                        .write_all(payload)
                        .await
                        .map_err(GossipError::Network)?;
                    bytes_written_counter.fetch_add(payload.len(), Ordering::Relaxed);
                    *bytes_since_flush += payload.len();
                }
            } else {
                let payload_offset = n - header.len();
                if payload_offset < payload.len() {
                    stream
                        .write_all(&payload[payload_offset..])
                        .await
                        .map_err(GossipError::Network)?;
                    bytes_written_counter
                        .fetch_add(payload.len() - payload_offset, Ordering::Relaxed);
                    *bytes_since_flush += payload.len() - payload_offset;
                }
            }
            Ok(())
        }
        Err(e) => Err(GossipError::Network(e)),
    }
}

async fn write_actor_response_direct<S>(
    stream: &mut S,
    bytes_written_counter: &Arc<AtomicUsize>,
    bytes_since_flush: &mut usize,
    correlation_id: u32,
    response: crate::registry::ActorResponse,
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    match response {
        crate::registry::ActorResponse::Bytes(bytes) => {
            let header = crate::framing::write_ask_response_header(
                crate::MessageType::Response,
                correlation_id,
                bytes.len(),
            );
            write_header_payload_vectored(
                stream,
                bytes_written_counter,
                bytes_since_flush,
                &header,
                &bytes,
            )
            .await?;
        }
        crate::registry::ActorResponse::Aligned(bytes) => {
            let header = crate::framing::write_ask_response_header(
                crate::MessageType::Response,
                correlation_id,
                bytes.len(),
            );
            write_header_payload_vectored(
                stream,
                bytes_written_counter,
                bytes_since_flush,
                &header,
                bytes.as_ref(),
            )
            .await?;
        }
        crate::registry::ActorResponse::Pooled {
            payload,
            prefix,
            payload_len,
        } => {
            let header = crate::framing::write_ask_response_header(
                crate::MessageType::Response,
                correlation_id,
                payload_len,
            );
            stream
                .write_all(&header)
                .await
                .map_err(GossipError::Network)?;
            bytes_written_counter.fetch_add(header.len(), Ordering::Relaxed);
            *bytes_since_flush += header.len();

            if let Some(prefix) = prefix {
                stream
                    .write_all(&prefix)
                    .await
                    .map_err(GossipError::Network)?;
                bytes_written_counter.fetch_add(prefix.len(), Ordering::Relaxed);
                *bytes_since_flush += prefix.len();
            }

            let mut payload = payload;
            while payload.has_remaining() {
                let chunk = payload.chunk();
                if chunk.is_empty() {
                    break;
                }
                stream
                    .write_all(chunk)
                    .await
                    .map_err(GossipError::Network)?;
                bytes_written_counter.fetch_add(chunk.len(), Ordering::Relaxed);
                *bytes_since_flush += chunk.len();
                payload.advance(chunk.len());
            }
        }
    }

    Ok(())
}

fn ask_context_from_context(
    ctx: &ReadContext,
    correlation_id: u32,
) -> Option<crate::AskContext<'_>> {
    ctx.response_writer
        .as_ref()
        .map(|writer| crate::AskContext::from_writer(correlation_id, writer, ctx.peer_id.as_ref()))
}

async fn write_ask_disposition_io<S>(
    ctx: &ReadContext,
    stream: &mut S,
    bytes_written_counter: &Arc<AtomicUsize>,
    bytes_since_flush: &mut usize,
    response_batch: &mut ResponseBatch,
    wrote_response_bytes: &mut bool,
    correlation_id: u32,
    disposition: crate::registry::AskDisposition,
    perf: Option<&IoPerfCounters>,
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    let write_start = perf.map(|_| Instant::now());
    let inline_payload_limit = ctx
        .max_message_size
        .saturating_sub(crate::framing::ASK_RESPONSE_HEADER_LEN);
    let schema_hash = ctx.expected_schema_hash;
    match disposition {
        crate::registry::AskDisposition::Deferred => {}
        crate::registry::AskDisposition::Immediate(response) => match response {
            crate::registry::ActorResponse::Bytes(payload) => {
                let should_stream =
                    payload.len() > inline_payload_limit || payload.len() > STREAMING_THRESHOLD;
                if should_stream {
                    write_streaming_response_direct(
                        stream,
                        bytes_written_counter,
                        bytes_since_flush,
                        correlation_id,
                        payload,
                        ctx.max_message_size,
                        schema_hash,
                    )
                    .await?;
                    *wrote_response_bytes = true;
                    if flush_each_actor_response() {
                        stream.flush().await.map_err(GossipError::Network)?;
                        *bytes_since_flush = 0;
                    }
                } else {
                    response_batch.push_bytes(correlation_id, payload);
                    *wrote_response_bytes = true;
                }
            }
            crate::registry::ActorResponse::Aligned(payload) => {
                let len = payload.len();
                let should_stream = len > inline_payload_limit || len > STREAMING_THRESHOLD;
                if should_stream {
                    write_streaming_response_direct(
                        stream,
                        bytes_written_counter,
                        bytes_since_flush,
                        correlation_id,
                        payload.into_bytes(),
                        ctx.max_message_size,
                        schema_hash,
                    )
                    .await?;
                    *wrote_response_bytes = true;
                    if flush_each_actor_response() {
                        stream.flush().await.map_err(GossipError::Network)?;
                        *bytes_since_flush = 0;
                    }
                } else {
                    response_batch.push_bytes(correlation_id, payload.into_bytes());
                    *wrote_response_bytes = true;
                }
            }
            other => {
                let should_stream = match &other {
                    crate::registry::ActorResponse::Pooled { payload_len, .. } => {
                        *payload_len > inline_payload_limit || *payload_len > STREAMING_THRESHOLD
                    }
                    _ => false,
                };
                if should_stream {
                    if let crate::registry::ActorResponse::Pooled {
                        payload,
                        prefix,
                        payload_len,
                    } = other
                    {
                        write_streaming_response_direct_pooled(
                            stream,
                            bytes_written_counter,
                            bytes_since_flush,
                            correlation_id,
                            payload,
                            prefix,
                            payload_len,
                            ctx.max_message_size,
                            schema_hash,
                        )
                        .await?;
                    } else {
                        let bytes = match other {
                            crate::registry::ActorResponse::Bytes(b) => b,
                            crate::registry::ActorResponse::Aligned(b) => b.into_bytes(),
                            crate::registry::ActorResponse::Pooled { .. } => unreachable!(),
                        };
                        write_streaming_response_direct(
                            stream,
                            bytes_written_counter,
                            bytes_since_flush,
                            correlation_id,
                            bytes,
                            ctx.max_message_size,
                            schema_hash,
                        )
                        .await?;
                    }
                    *wrote_response_bytes = true;
                    if flush_each_actor_response() {
                        stream.flush().await.map_err(GossipError::Network)?;
                        *bytes_since_flush = 0;
                    }
                } else {
                    write_actor_response_direct(
                        stream,
                        bytes_written_counter,
                        bytes_since_flush,
                        correlation_id,
                        other,
                    )
                    .await?;
                    *wrote_response_bytes = true;
                    if flush_each_actor_response() {
                        stream.flush().await.map_err(GossipError::Network)?;
                        *bytes_since_flush = 0;
                    }
                }
            }
        },
        crate::registry::AskDisposition::ImmediateBytes(payload) => {
            let should_stream =
                payload.len() > inline_payload_limit || payload.len() > STREAMING_THRESHOLD;
            if should_stream {
                write_streaming_response_direct(
                    stream,
                    bytes_written_counter,
                    bytes_since_flush,
                    correlation_id,
                    payload,
                    ctx.max_message_size,
                    schema_hash,
                )
                .await?;
                *wrote_response_bytes = true;
                if flush_each_actor_response() {
                    stream.flush().await.map_err(GossipError::Network)?;
                    *bytes_since_flush = 0;
                }
            } else {
                response_batch.push_bytes(correlation_id, payload);
                *wrote_response_bytes = true;
            }
        }
        crate::registry::AskDisposition::ImmediateAligned(payload) => {
            let len = payload.len();
            let should_stream = len > inline_payload_limit || len > STREAMING_THRESHOLD;
            if should_stream {
                write_streaming_response_direct(
                    stream,
                    bytes_written_counter,
                    bytes_since_flush,
                    correlation_id,
                    payload.into_bytes(),
                    ctx.max_message_size,
                    schema_hash,
                )
                .await?;
                *wrote_response_bytes = true;
                if flush_each_actor_response() {
                    stream.flush().await.map_err(GossipError::Network)?;
                    *bytes_since_flush = 0;
                }
            } else {
                response_batch.push_bytes(correlation_id, payload.into_bytes());
                *wrote_response_bytes = true;
            }
        }
        crate::registry::AskDisposition::ImmediatePooled {
            payload,
            prefix,
            payload_len,
        } => {
            let should_stream =
                payload_len > inline_payload_limit || payload_len > STREAMING_THRESHOLD;
            if should_stream {
                write_streaming_response_direct_pooled(
                    stream,
                    bytes_written_counter,
                    bytes_since_flush,
                    correlation_id,
                    payload,
                    prefix,
                    payload_len,
                    ctx.max_message_size,
                    schema_hash,
                )
                .await?;
                *wrote_response_bytes = true;
                if flush_each_actor_response() {
                    stream.flush().await.map_err(GossipError::Network)?;
                    *bytes_since_flush = 0;
                }
            } else {
                write_actor_response_direct(
                    stream,
                    bytes_written_counter,
                    bytes_since_flush,
                    correlation_id,
                    crate::registry::ActorResponse::Pooled {
                        payload,
                        prefix,
                        payload_len,
                    },
                )
                .await?;
                *wrote_response_bytes = true;
                if flush_each_actor_response() {
                    stream.flush().await.map_err(GossipError::Network)?;
                    *bytes_since_flush = 0;
                }
            }
        }
    }
    if let (Some(perf), Some(start)) = (perf, write_start) {
        perf.response_write_calls.fetch_add(1, Ordering::Relaxed);
        perf.response_write_ns
            .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }
    Ok(())
}

async fn write_streaming_response_direct<S>(
    stream: &mut S,
    bytes_written_counter: &Arc<AtomicUsize>,
    bytes_since_flush: &mut usize,
    correlation_id: u32,
    payload: bytes::Bytes,
    max_message_size: usize,
    _schema_hash: Option<u64>,
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    // A V5 data frame is [control:4][stream_id:4][chunk_index:4][payload].
    let max_chunk = max_message_size.saturating_sub(crate::framing::STREAM_RESPONSE_START_HEADER_LEN);
    if max_chunk == 0 {
        return Err(GossipError::InvalidConfig(format!(
            "max_message_size={} too small for streaming (overhead={})",
            max_message_size, crate::framing::STREAM_RESPONSE_START_HEADER_LEN
        )));
    }
    let chunk_size = std::cmp::min(STREAM_CHUNK_SIZE, max_chunk);
    let stream_id = allocate_direct_response_stream_id()?;
    let total_len = u32::try_from(payload.len()).map_err(|_| GossipError::MessageTooLarge {
        size: payload.len(),
        max: u32::MAX as usize,
    })?;
    let first_len = payload.len().min(chunk_size);
    let start_header = crate::framing::write_stream_response_start_header(
        stream_id,
        correlation_id,
        total_len,
        first_len,
    );
    write_header_payload_vectored(
        stream,
        bytes_written_counter,
        bytes_since_flush,
        &start_header,
        &payload[..first_len],
    )
    .await?;

    for (idx, chunk_data) in payload[first_len..].chunks(chunk_size).enumerate() {
        let chunk_index = u32::try_from(idx).map_err(|_| GossipError::MessageTooLarge {
            size: idx + 1,
            max: u32::MAX as usize,
        })? + 1;
        let header = crate::framing::write_stream_data_header(
            true,
            stream_id,
            chunk_index,
            chunk_data.len(),
        );
        write_header_payload_vectored(
            stream,
            bytes_written_counter,
            bytes_since_flush,
            &header,
            chunk_data,
        )
        .await?;
    }

    stream.flush().await.map_err(GossipError::Network)?;
    *bytes_since_flush = 0;

    Ok(())
}

async fn write_streaming_response_direct_pooled<S>(
    stream: &mut S,
    bytes_written_counter: &Arc<AtomicUsize>,
    bytes_since_flush: &mut usize,
    correlation_id: u32,
    mut payload: crate::typed::PooledPayload,
    prefix: Option<[u8; 16]>,
    payload_len: usize,
    max_message_size: usize,
    _schema_hash: Option<u64>,
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    let max_chunk = max_message_size
        .saturating_sub(crate::framing::STREAM_RESPONSE_START_HEADER_LEN);
    if max_chunk == 0 {
        return Err(GossipError::InvalidConfig(format!(
            "max_message_size={} too small for streaming (overhead={})",
            max_message_size, crate::framing::STREAM_RESPONSE_START_HEADER_LEN
        )));
    }
    let chunk_size = std::cmp::min(STREAM_CHUNK_SIZE, max_chunk);

    let prefix_len = prefix.as_ref().map(|p| p.len()).unwrap_or(0);
    if prefix_len > payload_len {
        return Err(GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "pooled response prefix_len exceeds payload_len",
        )));
    }
    let expected_payload_bytes = payload_len - prefix_len;
    if payload.remaining() < expected_payload_bytes {
        return Err(GossipError::Network(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "pooled response payload shorter than payload_len",
        )));
    }

    let stream_id = allocate_direct_response_stream_id()?;
    let total_len = u32::try_from(payload_len).map_err(|_| GossipError::MessageTooLarge {
        size: payload_len,
        max: u32::MAX as usize,
    })?;
    // StartData carries chunk zero; later frames carry only stream/index.
    let mut prefix_pos = 0usize;
    let prefix_bytes: Option<&[u8]> = prefix.as_ref().map(|p| p.as_slice());

    let mut remaining_total = payload_len;
    let mut idx = 0usize;
    while remaining_total > 0 {
        let this_chunk = std::cmp::min(chunk_size, remaining_total);

        let mut header_bytes = [0u8; 16];
        let header_len = if idx == 0 {
            let header = crate::framing::write_stream_response_start_header(
                stream_id,
                correlation_id,
                total_len,
                this_chunk,
            );
            header_bytes.copy_from_slice(&header);
            header.len()
        } else {
            let header = crate::framing::write_stream_data_header(
                true,
                stream_id,
                u32::try_from(idx).map_err(|_| GossipError::MessageTooLarge { size: idx, max: u32::MAX as usize })?,
                this_chunk,
            );
            header_bytes[..header.len()].copy_from_slice(&header);
            header.len()
        };

        stream
            .write_all(&header_bytes[..header_len])
            .await
            .map_err(GossipError::Network)?;
        bytes_written_counter.fetch_add(header_len, Ordering::Relaxed);
        *bytes_since_flush += header_len;

        // Write chunk bytes from: prefix (if any) then pooled payload Buf.
        let mut remaining_in_chunk = this_chunk;
        if let Some(prefix) = prefix_bytes {
            if prefix_pos < prefix.len() && remaining_in_chunk > 0 {
                let take = std::cmp::min(remaining_in_chunk, prefix.len() - prefix_pos);
                stream
                    .write_all(&prefix[prefix_pos..prefix_pos + take])
                    .await
                    .map_err(GossipError::Network)?;
                bytes_written_counter.fetch_add(take, Ordering::Relaxed);
                *bytes_since_flush += take;
                prefix_pos += take;
                remaining_in_chunk -= take;
            }
        }

        while remaining_in_chunk > 0 {
            let chunk = payload.chunk();
            if chunk.is_empty() {
                return Err(GossipError::Network(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "pooled payload returned empty chunk while bytes remain",
                )));
            }
            let take = std::cmp::min(remaining_in_chunk, chunk.len());
            stream
                .write_all(&chunk[..take])
                .await
                .map_err(GossipError::Network)?;
            bytes_written_counter.fetch_add(take, Ordering::Relaxed);
            *bytes_since_flush += take;
            payload.advance(take);
            remaining_in_chunk -= take;
        }

        remaining_total -= this_chunk;
        idx += 1;
    }

    stream.flush().await.map_err(GossipError::Network)?;
    *bytes_since_flush = 0;

    Ok(())
}

async fn process_read_result_io<S>(
    result: crate::handle::MessageReadResult,
    streaming_state: &mut crate::protocol::StreamingState,
    registry: &Arc<GossipRegistry>,
    peer_addr: SocketAddr,
    // R-11: this connection's own session discriminator (see
    // `ReadContext::session_source`), threaded down to `merge_full_sync_from`
    // so the restart-sequence exemption is scoped to the exact connection
    // that armed it, not merely to `peer_addr` (which for an outbound
    // connection is the peer's fixed listening port, shared by every
    // connection we ever make to it).
    session_source: SocketAddr,
    authenticated_peer_id: Option<&crate::PeerId>,
    response_correlation: Option<&CorrelationTracker>,
    sync_actor_handler: Option<&crate::registry::ActorMessageHandlerSyncCell>,
    stream: &mut S,
    bytes_written_counter: &Arc<AtomicUsize>,
    bytes_since_flush: &mut usize,
    response_batch: &mut ResponseBatch,
    direct_response_batch: &mut DirectResponseBatch,
    perf: Option<&IoPerfCounters>,
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    match result {
        crate::handle::MessageReadResult::Actor {
            msg_type,
            correlation_id,
            actor_id,
            type_hash,
            schema_hash,
            payload,
        } => {
            if let (Some(expected), Some(received)) = (registry.config.schema_hash, schema_hash) {
                // V5 authenticates the schema during Hello, so normal V5
                // actor frames intentionally carry no repeated schema hash.
                if received != expected {
                    warn!(
                        peer = %peer_addr,
                        expected = format_args!("{:016x}", expected),
                        received = format_args!("{received:016x}"),
                        "Rejected actor payload due to schema hash mismatch"
                    );
                    return Ok(());
                }
            }

            let corr_id = if msg_type == crate::MessageType::ActorAsk as u8 {
                correlation_id
            } else {
                0
            };
            let correlation_opt = if corr_id == 0 { None } else { Some(corr_id) };
            if msg_type == crate::MessageType::ActorTell as u8
                && let Some(cell) = registry.actor_tell_handler_sync_context.load_full()
            {
                let handle_start = perf.map(|_| Instant::now());
                cell.handle(
                    actor_id,
                    type_hash,
                    payload,
                    crate::TellContext::new(authenticated_peer_id),
                )?;
                if let (Some(perf), Some(start)) = (perf, handle_start) {
                    perf.actor_handle_calls.fetch_add(1, Ordering::Relaxed);
                    perf.actor_handle_ns
                        .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                }
                return Ok(());
            }
            if msg_type == crate::MessageType::ActorTell as u8
                && let Some(cell) = registry.actor_tell_handler_sync.load_full()
            {
                let handle_start = perf.map(|_| Instant::now());
                cell.handle(actor_id, type_hash, payload)?;
                if let (Some(perf), Some(start)) = (perf, handle_start) {
                    perf.actor_handle_calls.fetch_add(1, Ordering::Relaxed);
                    perf.actor_handle_ns
                        .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                }
                return Ok(());
            }

            let handle_start = perf.map(|_| Instant::now());
            let response = if let Some(cell) = sync_actor_handler {
                cell.handle(actor_id, type_hash, payload, correlation_opt)
            } else {
                registry
                    .handle_actor_message(actor_id, type_hash, payload, correlation_opt)
                    .await
            };
            if let (Some(perf), Some(start)) = (perf, handle_start) {
                perf.actor_handle_calls.fetch_add(1, Ordering::Relaxed);
                perf.actor_handle_ns
                    .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
            if let Ok(Some(response)) = response
                && corr_id != 0
            {
                let temp_ctx = ReadContext {
                    streaming_state_handoff: None,
                    registry_weak: Arc::downgrade(registry),
                    peer_addr,
                    session_source: peer_addr,
                    peer_id: None,
                    max_message_size: registry.config.max_message_size,
                    expected_schema_hash: registry.config.schema_hash,
                    aligned_pool: registry.connection_pool.aligned_bytes_pool(),
                    inbound_routes: Arc::new(crate::route_interning::RouteTable::new()),
                    response_correlation: None,
                    response_writer: None,
                    tell_handler_sync: None,
            tell_handler_sync_context: None,
                    ask_immediate_handler_sync: None,
                    ask_handler_sync: None,
                    sync_actor_handler: None,
                };
                let mut wrote_response_bytes = false;
                write_ask_disposition_io(
                    &temp_ctx,
                    stream,
                    bytes_written_counter,
                    bytes_since_flush,
                    response_batch,
                    &mut wrote_response_bytes,
                    corr_id,
                    crate::registry::AskDisposition::Immediate(response),
                    perf,
                )
                .await?;
            }
            Ok(())
        }
        crate::handle::MessageReadResult::DirectAsk {
            correlation_id,
            payload,
        } => {
            // DirectAsk has no registered application handler; production
            // builds must not fabricate a response from the request bytes.
            #[cfg(any(test, feature = "test-helpers", debug_assertions))]
            {
                let write_start = perf.map(|_| Instant::now());
                direct_response_batch.push_bytes(correlation_id, payload.into_bytes());
                if let (Some(perf), Some(start)) = (perf, write_start) {
                    perf.response_write_calls.fetch_add(1, Ordering::Relaxed);
                    perf.response_write_ns
                        .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                }
            }
            #[cfg(not(any(test, feature = "test-helpers", debug_assertions)))]
            {
                let _ = payload;
                let _ = &direct_response_batch;
                warn!(
                    peer = %peer_addr,
                    correlation_id,
                    "Received DirectAsk request - no handler registered, dropping"
                );
            }
            Ok(())
        }
        crate::handle::MessageReadResult::DirectResponse {
            correlation_id,
            payload,
        } => {
            crate::handle::handle_response_message(
                registry,
                peer_addr,
                correlation_id,
                payload,
                response_correlation,
            )
            .await;
            Ok(())
        }
        other => {
            crate::protocol::process_read_result(
                other,
                streaming_state,
                registry,
                peer_addr,
                session_source,
                response_correlation,
                None,
                authenticated_peer_id,
            )
            .await
        }
    }
}

async fn try_handle_fast_io<S>(
    result: ReadIoResult,
    ctx: &ReadContext,
    stream: &mut S,
    bytes_written_counter: &Arc<AtomicUsize>,
    bytes_since_flush: &mut usize,
    response_batch: &mut ResponseBatch,
    direct_response_batch: &mut DirectResponseBatch,
    wrote_response_bytes: &mut bool,
    perf: Option<&IoPerfCounters>,
) -> Result<Option<crate::handle::MessageReadResult>>
where
    S: AsyncWrite + Unpin,
{
    async fn handle_fast_actor_sync_io<S>(
        ctx: &ReadContext,
        msg_type: u8,
        actor_id: u64,
        type_hash: u32,
        payload: crate::AlignedBytes,
        correlation_id: Option<u32>,
        stream: &mut S,
        bytes_written_counter: &Arc<AtomicUsize>,
        bytes_since_flush: &mut usize,
        response_batch: &mut ResponseBatch,
        wrote_response_bytes: &mut bool,
        perf: Option<&IoPerfCounters>,
    ) -> Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        if msg_type == crate::MessageType::ActorAsk as u8
            && ctx.ask_immediate_handler_sync.is_none()
            && ctx.ask_handler_sync.is_none()
            && let Some(cell) = ctx.sync_actor_handler.as_ref()
        {
            let handle_start = perf.map(|_| Instant::now());
            let response = cell.handle(actor_id, type_hash, payload, correlation_id);
            if let (Some(perf), Some(start)) = (perf, handle_start) {
                perf.actor_handle_calls.fetch_add(1, Ordering::Relaxed);
                perf.actor_handle_ns
                    .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
            if let Some(correlation_id) = correlation_id
                && let Ok(Some(response)) = response
            {
                write_ask_disposition_io(
                    ctx,
                    stream,
                    bytes_written_counter,
                    bytes_since_flush,
                    response_batch,
                    wrote_response_bytes,
                    correlation_id,
                    crate::registry::AskDisposition::Immediate(response),
                    perf,
                )
                .await?;
            }
            return Ok(());
        }

        if msg_type == crate::MessageType::ActorTell as u8 {
            if let Some(cell) = ctx.tell_handler_sync_context.as_ref() {
                let handle_start = perf.map(|_| Instant::now());
                let result = cell.handle(
                    actor_id,
                    type_hash,
                    payload,
                    crate::TellContext::new(ctx.peer_id.as_ref()),
                );
                if let (Some(perf), Some(start)) = (perf, handle_start) {
                    perf.actor_handle_calls.fetch_add(1, Ordering::Relaxed);
                    perf.actor_handle_ns
                        .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                }
                result?;
                return Ok(());
            }
            if let Some(cell) = ctx.tell_handler_sync.as_ref() {
                let handle_start = perf.map(|_| Instant::now());
                let result = cell.handle(actor_id, type_hash, payload);
                if let (Some(perf), Some(start)) = (perf, handle_start) {
                    perf.actor_handle_calls.fetch_add(1, Ordering::Relaxed);
                    perf.actor_handle_ns
                        .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                }
                result?;
                return Ok(());
            }
        } else if msg_type == crate::MessageType::ActorAsk as u8
            && let Some(correlation_id) = correlation_id
        {
            if let Some(cell) = ctx.ask_immediate_handler_sync.as_ref()
                && cell.can_handle(actor_id, type_hash)
            {
                let handle_start = perf.map(|_| Instant::now());
                let disposition = cell.handle(actor_id, type_hash, payload)?;
                if let (Some(perf), Some(start)) = (perf, handle_start) {
                    perf.actor_handle_calls.fetch_add(1, Ordering::Relaxed);
                    perf.actor_handle_ns
                        .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                }
                write_ask_disposition_io(
                    ctx,
                    stream,
                    bytes_written_counter,
                    bytes_since_flush,
                    response_batch,
                    wrote_response_bytes,
                    correlation_id,
                    disposition,
                    perf,
                )
                .await?;
                return Ok(());
            }
            if let Some(cell) = ctx.ask_handler_sync.as_ref()
                && let Some(context) = ask_context_from_context(ctx, correlation_id)
            {
                let handle_start = perf.map(|_| Instant::now());
                let disposition = cell.handle(actor_id, type_hash, payload, context)?;
                if let (Some(perf), Some(start)) = (perf, handle_start) {
                    perf.actor_handle_calls.fetch_add(1, Ordering::Relaxed);
                    perf.actor_handle_ns
                        .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                }
                write_ask_disposition_io(
                    ctx,
                    stream,
                    bytes_written_counter,
                    bytes_since_flush,
                    response_batch,
                    wrote_response_bytes,
                    correlation_id,
                    disposition,
                    perf,
                )
                .await?;
                return Ok(());
            }
        }

        let Some(cell) = ctx.sync_actor_handler.as_ref() else {
            return Ok(());
        };
        let handle_start = perf.map(|_| Instant::now());
        let response = cell.handle(actor_id, type_hash, payload, correlation_id);
        if let (Some(perf), Some(start)) = (perf, handle_start) {
            perf.actor_handle_calls.fetch_add(1, Ordering::Relaxed);
            perf.actor_handle_ns
                .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }

        if let Some(correlation_id) = correlation_id
            && let Ok(Some(response)) = response
        {
            write_ask_disposition_io(
                ctx,
                stream,
                bytes_written_counter,
                bytes_since_flush,
                response_batch,
                wrote_response_bytes,
                correlation_id,
                crate::registry::AskDisposition::Immediate(response),
                perf,
            )
            .await?;
        }

        Ok(())
    }

    match result {
        ReadIoResult::DirectAsk {
            correlation_id,
            payload,
        } => {
            // DirectAsk has no registered application handler; production
            // builds must not fabricate a response from the request bytes.
            #[cfg(any(test, feature = "test-helpers", debug_assertions))]
            {
                let write_start = perf.map(|_| Instant::now());
                direct_response_batch.push_bytes(correlation_id, payload.into_bytes());
                *wrote_response_bytes = true;
                if let (Some(perf), Some(start)) = (perf, write_start) {
                    perf.response_write_calls.fetch_add(1, Ordering::Relaxed);
                    perf.response_write_ns
                        .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                }
            }
            #[cfg(not(any(test, feature = "test-helpers", debug_assertions)))]
            {
                let _ = payload;
                let _ = &direct_response_batch;
                warn!(
                    peer = %ctx.peer_addr,
                    correlation_id,
                    "Received DirectAsk request - no handler registered, dropping"
                );
            }
            Ok(None)
        }
        ReadIoResult::ActorAsk {
            correlation_id,
            actor_id,
            type_hash,
            payload,
        } => {
            if ctx.ask_immediate_handler_sync.is_none()
                && ctx.ask_handler_sync.is_none()
                && ctx.sync_actor_handler.is_none()
            {
                return Ok(Some(crate::handle::MessageReadResult::Actor {
                    msg_type: crate::MessageType::ActorAsk as u8,
                    correlation_id,
                    actor_id,
                    type_hash,
                    schema_hash: ctx.expected_schema_hash,
                    payload,
                }));
            }
            handle_fast_actor_sync_io(
                ctx,
                crate::MessageType::ActorAsk as u8,
                actor_id,
                type_hash,
                payload,
                Some(correlation_id),
                stream,
                bytes_written_counter,
                bytes_since_flush,
                response_batch,
                wrote_response_bytes,
                perf,
            )
            .await?;
            Ok(None)
        }
        ReadIoResult::Generic(crate::handle::MessageReadResult::Actor {
            msg_type,
            correlation_id,
            actor_id,
            type_hash,
            schema_hash,
            payload,
        }) => {
            let is_tell = msg_type == crate::MessageType::ActorTell as u8 && correlation_id == 0;
            let is_ask = msg_type == crate::MessageType::ActorAsk as u8 && correlation_id != 0;
            let has_split = (is_tell && ctx.tell_handler_sync.is_some())
                || (is_ask
                    && (ctx.ask_immediate_handler_sync.is_some()
                        || ctx.ask_handler_sync.is_some()));
            let has_context_tell = is_tell && ctx.tell_handler_sync_context.is_some();
            let has_legacy = ctx.sync_actor_handler.is_some() && (is_tell || is_ask);
            if !has_split && !has_context_tell && !has_legacy {
                return Ok(Some(crate::handle::MessageReadResult::Actor {
                    msg_type,
                    correlation_id,
                    actor_id,
                    type_hash,
                    schema_hash,
                    payload,
                }));
            }

            if let (Some(expected), Some(received)) = (ctx.expected_schema_hash, schema_hash)
                && received != expected
            {
                return Ok(None);
            }
            handle_fast_actor_sync_io(
                ctx,
                msg_type,
                actor_id,
                type_hash,
                payload,
                if is_ask { Some(correlation_id) } else { None },
                stream,
                bytes_written_counter,
                bytes_since_flush,
                response_batch,
                wrote_response_bytes,
                perf,
            )
            .await?;
            Ok(None)
        }
        ReadIoResult::Generic(crate::handle::MessageReadResult::DirectAsk {
            correlation_id,
            payload,
        }) => {
            // DirectAsk has no registered application handler; production
            // builds must not fabricate a response from the request bytes.
            #[cfg(any(test, feature = "test-helpers", debug_assertions))]
            {
                let write_start = perf.map(|_| Instant::now());
                direct_response_batch.push_bytes(correlation_id, payload.into_bytes());
                *wrote_response_bytes = true;
                if let (Some(perf), Some(start)) = (perf, write_start) {
                    perf.response_write_calls.fetch_add(1, Ordering::Relaxed);
                    perf.response_write_ns
                        .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                }
            }
            #[cfg(not(any(test, feature = "test-helpers", debug_assertions)))]
            {
                let _ = payload;
                let _ = &direct_response_batch;
                warn!(
                    peer = %ctx.peer_addr,
                    correlation_id,
                    "Received DirectAsk request - no handler registered, dropping"
                );
            }
            Ok(None)
        }
        ReadIoResult::Generic(crate::handle::MessageReadResult::DirectResponse {
            correlation_id,
            payload,
        }) => {
            let mut payload = Some(payload);
            if let Some(correlation) = ctx.response_correlation.as_deref()
                && correlation.complete(correlation_id, &mut payload)
            {
                return Ok(None);
            }
            Ok(Some(crate::handle::MessageReadResult::DirectResponse {
                correlation_id,
                payload: payload.expect("payload retained when direct response was not consumed"),
            }))
        }
        ReadIoResult::Generic(crate::handle::MessageReadResult::Response {
            correlation_id,
            payload,
        }) => {
            let mut payload = Some(payload);
            if let Some(correlation) = ctx.response_correlation.as_deref()
                && correlation.complete(correlation_id, &mut payload)
            {
                return Ok(None);
            }
            Ok(Some(crate::handle::MessageReadResult::Response {
                correlation_id,
                payload: payload.expect("payload retained when response was not consumed"),
            }))
        }
        ReadIoResult::Generic(other) => Ok(Some(other)),
    }
}
