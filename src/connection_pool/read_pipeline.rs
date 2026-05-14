#[derive(Clone)]
#[doc(hidden)]
pub struct ReadContext {
    pub(crate) registry_weak: std::sync::Weak<GossipRegistry>,
    pub(crate) peer_addr: SocketAddr,
    /// Best-effort peer identity for this connection.
    ///
    /// This is used to avoid mis-attributing disconnects from stale/duplicate
    /// connections (for example tie-breaker drops during simultaneous dial).
    pub(crate) peer_id: Option<crate::PeerId>,
    pub(crate) max_message_size: usize,
    pub(crate) expected_schema_hash: Option<u64>,
    pub(crate) aligned_pool: Arc<crate::AlignedBytesPool>,
    pub(crate) response_correlation: Option<Arc<CorrelationTracker>>,
    pub(crate) response_writer: Option<Arc<crate::ask_responder::ResponseWriter>>,
    pub(crate) tell_handler_sync: Option<Arc<crate::registry::ActorTellHandlerSyncCell>>,
    pub(crate) ask_immediate_handler_sync:
        Option<Arc<crate::registry::ActorAskImmediateHandlerSyncCell>>,
    pub(crate) ask_handler_sync: Option<Arc<crate::registry::ActorAskHandlerSyncCell>>,
    pub(crate) sync_actor_handler: Option<Arc<crate::registry::ActorMessageHandlerSyncCell>>,
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

            let msg_len = u32::from_be_bytes(*buf) as usize;
            if msg_len > ctx.max_message_size {
                return Err(GossipError::MessageTooLarge {
                    size: msg_len,
                    max: ctx.max_message_size,
                });
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

            let result = crate::handle::parse_message_from_pooled_buffer(buffer, msg_len)?;
            Ok(Some(result))
        }
    }
}

struct ReadPollResult {
    result: Option<ReadIoResult>,
    progressed: bool,
}

enum ReadIoResult {
    Generic(crate::handle::MessageReadResult),
    DirectAsk {
        correlation_id: u16,
        payload: crate::AlignedBytes,
    },
    ActorAsk {
        correlation_id: u16,
        actor_id: u64,
        type_hash: u32,
        payload: crate::AlignedBytes,
    },
}

enum FastReadOutcome {
    Handled,
    Parsed(ReadIoResult),
    Unhandled(crate::PooledAlignedBuffer),
}

fn try_handle_read_fast_from_pooled(
    buffer: crate::PooledAlignedBuffer,
    msg_len: usize,
    ctx: &ReadContext,
) -> Result<FastReadOutcome> {
    let msg_data = &buffer.as_ref()[crate::framing::LENGTH_PREFIX_LEN..];

    if let Some(correlation) = ctx.response_correlation.as_deref() {
        if msg_len >= crate::framing::ASK_RESPONSE_HEADER_LEN
            && msg_data[0] == crate::MessageType::Response as u8
        {
            let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);
            let payload_len = msg_len - crate::framing::ASK_RESPONSE_HEADER_LEN;
            let payload_offset =
                crate::framing::LENGTH_PREFIX_LEN + crate::framing::ASK_RESPONSE_HEADER_LEN;
            let mut payload = Some(crate::AlignedBytes::from_pooled_buffer_range(
                buffer,
                payload_offset,
                payload_len,
            )?);
            if correlation.complete(correlation_id, &mut payload) {
                return Ok(FastReadOutcome::Handled);
            }
            return Ok(FastReadOutcome::Parsed(ReadIoResult::Generic(
                crate::handle::MessageReadResult::Response {
                    correlation_id,
                    payload: payload.expect("payload retained when response was not consumed"),
                },
            )));
        }

        if msg_len >= crate::framing::DIRECT_RESPONSE_HEADER_LEN
            && msg_data[0] == crate::MessageType::DirectResponse as u8
        {
            let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);
            let payload_len =
                u32::from_be_bytes([msg_data[3], msg_data[4], msg_data[5], msg_data[6]]) as usize;
            if msg_data.len() < crate::framing::DIRECT_RESPONSE_HEADER_LEN + payload_len {
                return Ok(FastReadOutcome::Unhandled(buffer));
            }
            let payload_offset =
                crate::framing::LENGTH_PREFIX_LEN + crate::framing::DIRECT_RESPONSE_HEADER_LEN;
            let mut payload = Some(crate::AlignedBytes::from_pooled_buffer_range(
                buffer,
                payload_offset,
                payload_len,
            )?);
            if correlation.complete(correlation_id, &mut payload) {
                return Ok(FastReadOutcome::Handled);
            }
            return Ok(FastReadOutcome::Parsed(ReadIoResult::Generic(
                crate::handle::MessageReadResult::DirectResponse {
                    correlation_id,
                    payload: payload
                        .expect("payload retained when direct response was not consumed"),
                },
            )));
        }
    }

    if msg_len >= crate::framing::DIRECT_ASK_HEADER_LEN
        && msg_data[0] == crate::MessageType::DirectAsk as u8
    {
        let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);
        let payload_len =
            u32::from_be_bytes([msg_data[3], msg_data[4], msg_data[5], msg_data[6]]) as usize;
        if msg_data.len() < crate::framing::DIRECT_ASK_HEADER_LEN + payload_len {
            return Ok(FastReadOutcome::Unhandled(buffer));
        }
        let payload_offset =
            crate::framing::LENGTH_PREFIX_LEN + crate::framing::DIRECT_ASK_HEADER_LEN;
        let payload =
            crate::AlignedBytes::from_pooled_buffer_range(buffer, payload_offset, payload_len)?;
        return Ok(FastReadOutcome::Parsed(ReadIoResult::DirectAsk {
            correlation_id,
            payload,
        }));
    }

    if msg_len < crate::framing::ACTOR_HEADER_LEN {
        return Ok(FastReadOutcome::Unhandled(buffer));
    }

    if msg_data[0] != crate::MessageType::ActorTell as u8 {
        if msg_data[0] != crate::MessageType::ActorAsk as u8 {
            return Ok(FastReadOutcome::Unhandled(buffer));
        }
        if ctx.ask_immediate_handler_sync.is_none()
            && ctx.ask_handler_sync.is_none()
            && ctx.sync_actor_handler.is_none()
        {
            return Ok(FastReadOutcome::Unhandled(buffer));
        }
        let correlation_id = u16::from_be_bytes([msg_data[1], msg_data[2]]);
        if correlation_id == 0 {
            return Ok(FastReadOutcome::Unhandled(buffer));
        }
        if let Some(expected) = ctx.expected_schema_hash {
            let schema_hash = crate::framing::read_schema_hash(&msg_data[3..12]);
            if schema_hash != Some(expected) {
                return Ok(FastReadOutcome::Handled);
            }
        }
        let actor_id = u64::from_be_bytes(msg_data[12..20].try_into().unwrap());
        let type_hash = u32::from_be_bytes(msg_data[20..24].try_into().unwrap());
        let payload_len =
            u32::from_be_bytes([msg_data[24], msg_data[25], msg_data[26], msg_data[27]]) as usize;
        if msg_data.len() < crate::framing::ACTOR_HEADER_LEN + payload_len {
            return Ok(FastReadOutcome::Unhandled(buffer));
        }
        let payload_offset = crate::framing::LENGTH_PREFIX_LEN + crate::framing::ACTOR_HEADER_LEN;
        let payload =
            crate::AlignedBytes::from_pooled_buffer_range(buffer, payload_offset, payload_len)?;
        return Ok(FastReadOutcome::Parsed(ReadIoResult::ActorAsk {
            correlation_id,
            actor_id,
            type_hash,
            payload,
        }));
    }
    if ctx.tell_handler_sync.is_none() && ctx.sync_actor_handler.is_none() {
        return Ok(FastReadOutcome::Unhandled(buffer));
    }
    if msg_data[1] != 0 || msg_data[2] != 0 {
        return Ok(FastReadOutcome::Unhandled(buffer));
    }

    if let Some(expected) = ctx.expected_schema_hash {
        let schema_hash = crate::framing::read_schema_hash(&msg_data[3..12]);
        if schema_hash != Some(expected) {
            return Ok(FastReadOutcome::Handled);
        }
    }

    let actor_id = u64::from_be_bytes(msg_data[12..20].try_into().unwrap());
    let type_hash = u32::from_be_bytes(msg_data[20..24].try_into().unwrap());
    let payload_len =
        u32::from_be_bytes([msg_data[24], msg_data[25], msg_data[26], msg_data[27]]) as usize;

    if msg_data.len() < crate::framing::ACTOR_HEADER_LEN + payload_len {
        return Ok(FastReadOutcome::Unhandled(buffer));
    }

    let payload_offset = crate::framing::LENGTH_PREFIX_LEN + crate::framing::ACTOR_HEADER_LEN;
    let payload =
        crate::AlignedBytes::from_pooled_buffer_range(buffer, payload_offset, payload_len)?;
    if let Some(cell) = ctx.tell_handler_sync.as_ref() {
        let _ = cell.handle(actor_id, type_hash, payload);
        return Ok(FastReadOutcome::Handled);
    }
    if let Some(cell) = ctx.sync_actor_handler.as_ref() {
        let _ = cell.handle(actor_id, type_hash, payload, None);
        return Ok(FastReadOutcome::Handled);
    }
    unreachable!("actor tell path checked handler presence before payload extraction")
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

                    let msg_len = u32::from_be_bytes(*buf) as usize;
                    if msg_len > ctx.max_message_size {
                        return Poll::Ready(Err(GossipError::MessageTooLarge {
                            size: msg_len,
                            max: ctx.max_message_size,
                        }));
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
                            crate::handle::parse_message_from_pooled_buffer(buffer, msg_len)?,
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

                    let msg_len = u32::from_be_bytes(*buf) as usize;
                    if msg_len > ctx.max_message_size {
                        return Poll::Ready(Err(GossipError::MessageTooLarge {
                            size: msg_len,
                            max: ctx.max_message_size,
                        }));
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
                            crate::handle::parse_message_from_pooled_buffer(buffer, msg_len)?,
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
    correlation_id: u16,
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
    correlation_id: u16,
) -> Option<crate::AskContext<'_>> {
    ctx.response_writer
        .as_ref()
        .map(|writer| crate::AskContext::from_writer(correlation_id, writer))
}

async fn write_ask_disposition_io<S>(
    ctx: &ReadContext,
    stream: &mut S,
    bytes_written_counter: &Arc<AtomicUsize>,
    bytes_since_flush: &mut usize,
    response_batch: &mut ResponseBatch,
    wrote_response_bytes: &mut bool,
    correlation_id: u16,
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
    correlation_id: u16,
    payload: bytes::Bytes,
    max_message_size: usize,
    schema_hash: Option<u64>,
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    use bytes::BufMut;

    // Streaming frame wire format excludes the 4-byte length prefix from `msg_len`.
    // `msg_len` for stream data frames is: type(1) + corr(2) + reserved(9) + header(36) + chunk(N).
    const STREAM_FRAME_OVERHEAD: usize = 12 + crate::StreamHeader::SERIALIZED_SIZE;
    let max_chunk = max_message_size.saturating_sub(STREAM_FRAME_OVERHEAD);
    if max_chunk == 0 {
        return Err(GossipError::InvalidConfig(format!(
            "max_message_size={} too small for streaming (overhead={})",
            max_message_size, STREAM_FRAME_OVERHEAD
        )));
    }
    let chunk_size = std::cmp::min(STREAM_CHUNK_SIZE, max_chunk);

    // Generate unique stream ID for this response stream.
    let stream_id = crate::current_timestamp_nanos();

    fn build_stream_response_header(
        msg_type: crate::MessageType,
        header: &crate::StreamHeader,
        correlation_id: u16,
        chunk_len: usize,
        schema_hash: Option<u64>,
    ) -> bytes::Bytes {
        // Message format: [length:4][type:1][correlation_id:2][reserved:9][header:36]
        let inner_size = 12 + crate::StreamHeader::SERIALIZED_SIZE + chunk_len;
        let mut message =
            bytes::BytesMut::with_capacity(4 + 12 + crate::StreamHeader::SERIALIZED_SIZE);

        message.put_u32(inner_size as u32);
        message.put_u8(msg_type as u8);
        message.put_u16(correlation_id);

        let mut reserved = [0u8; 9];
        crate::framing::write_schema_hash(&mut reserved, schema_hash);
        message.put_slice(&reserved);
        message.put_slice(&header.to_bytes());

        message.freeze()
    }

    let total_len = payload.len();
    let start_header = crate::StreamHeader {
        stream_id,
        total_size: total_len as u64,
        chunk_size: 0,
        chunk_index: 0,
        type_hash: 0,
        actor_id: 0,
    };

    // StreamResponseStart
    let start_msg = build_stream_response_header(
        crate::MessageType::StreamResponseStart,
        &start_header,
        correlation_id,
        0,
        schema_hash,
    );
    stream
        .write_all(&start_msg)
        .await
        .map_err(GossipError::Network)?;
    bytes_written_counter.fetch_add(start_msg.len(), Ordering::Relaxed);
    *bytes_since_flush += start_msg.len();

    // StreamResponseData
    let num_chunks = total_len.div_ceil(chunk_size);
    for idx in 0..num_chunks {
        let start = idx * chunk_size;
        let end = std::cmp::min(start + chunk_size, total_len);
        let chunk_len = end - start;
        let chunk_data = payload.slice(start..end);

        let data_header = crate::StreamHeader {
            stream_id,
            total_size: total_len as u64,
            chunk_size: chunk_len as u32,
            chunk_index: idx as u32,
            type_hash: 0,
            actor_id: 0,
        };

        let header_bytes = build_stream_response_header(
            crate::MessageType::StreamResponseData,
            &data_header,
            correlation_id,
            chunk_len,
            schema_hash,
        );

        write_header_payload_vectored(
            stream,
            bytes_written_counter,
            bytes_since_flush,
            &header_bytes,
            chunk_data.as_ref(),
        )
        .await?;
    }

    // StreamResponseEnd
    let end_msg = build_stream_response_header(
        crate::MessageType::StreamResponseEnd,
        &start_header,
        correlation_id,
        0,
        schema_hash,
    );
    stream
        .write_all(&end_msg)
        .await
        .map_err(GossipError::Network)?;
    bytes_written_counter.fetch_add(end_msg.len(), Ordering::Relaxed);
    *bytes_since_flush += end_msg.len();

    stream.flush().await.map_err(GossipError::Network)?;
    *bytes_since_flush = 0;

    Ok(())
}

async fn write_streaming_response_direct_pooled<S>(
    stream: &mut S,
    bytes_written_counter: &Arc<AtomicUsize>,
    bytes_since_flush: &mut usize,
    correlation_id: u16,
    mut payload: crate::typed::PooledPayload,
    prefix: Option<[u8; 16]>,
    payload_len: usize,
    max_message_size: usize,
    schema_hash: Option<u64>,
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    use bytes::BufMut;

    // Streaming frame wire format excludes the 4-byte length prefix from `msg_len`.
    // `msg_len` for stream data frames is: type(1) + corr(2) + reserved(9) + header(36) + chunk(N).
    const STREAM_FRAME_OVERHEAD: usize = 12 + crate::StreamHeader::SERIALIZED_SIZE;
    let max_chunk = max_message_size.saturating_sub(STREAM_FRAME_OVERHEAD);
    if max_chunk == 0 {
        return Err(GossipError::InvalidConfig(format!(
            "max_message_size={} too small for streaming (overhead={})",
            max_message_size, STREAM_FRAME_OVERHEAD
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

    // Generate unique stream ID for this response stream.
    let stream_id = crate::current_timestamp_nanos();

    fn build_stream_response_header(
        msg_type: crate::MessageType,
        header: &crate::StreamHeader,
        correlation_id: u16,
        chunk_len: usize,
        schema_hash: Option<u64>,
    ) -> bytes::Bytes {
        // Message format: [length:4][type:1][correlation_id:2][reserved:9][header:36]
        let inner_size = 12 + crate::StreamHeader::SERIALIZED_SIZE + chunk_len;
        let mut message =
            bytes::BytesMut::with_capacity(4 + 12 + crate::StreamHeader::SERIALIZED_SIZE);

        message.put_u32(inner_size as u32);
        message.put_u8(msg_type as u8);
        message.put_u16(correlation_id);

        let mut reserved = [0u8; 9];
        crate::framing::write_schema_hash(&mut reserved, schema_hash);
        message.put_slice(&reserved);
        message.put_slice(&header.to_bytes());

        message.freeze()
    }

    let total_len = payload_len;
    let start_header = crate::StreamHeader {
        stream_id,
        total_size: total_len as u64,
        chunk_size: 0,
        chunk_index: 0,
        type_hash: 0,
        actor_id: 0,
    };

    // StreamResponseStart
    let start_msg = build_stream_response_header(
        crate::MessageType::StreamResponseStart,
        &start_header,
        correlation_id,
        0,
        schema_hash,
    );
    stream
        .write_all(&start_msg)
        .await
        .map_err(GossipError::Network)?;
    bytes_written_counter.fetch_add(start_msg.len(), Ordering::Relaxed);
    *bytes_since_flush += start_msg.len();

    // StreamResponseData
    let mut prefix_pos = 0usize;
    let prefix_bytes: Option<&[u8]> = prefix.as_ref().map(|p| p.as_slice());

    let mut remaining_total = total_len;
    let mut idx = 0usize;
    while remaining_total > 0 {
        let this_chunk = std::cmp::min(chunk_size, remaining_total);

        let data_header = crate::StreamHeader {
            stream_id,
            total_size: total_len as u64,
            chunk_size: this_chunk as u32,
            chunk_index: idx as u32,
            type_hash: 0,
            actor_id: 0,
        };

        let header_bytes = build_stream_response_header(
            crate::MessageType::StreamResponseData,
            &data_header,
            correlation_id,
            this_chunk,
            schema_hash,
        );

        stream
            .write_all(&header_bytes)
            .await
            .map_err(GossipError::Network)?;
        bytes_written_counter.fetch_add(header_bytes.len(), Ordering::Relaxed);
        *bytes_since_flush += header_bytes.len();

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

    // StreamResponseEnd
    let end_msg = build_stream_response_header(
        crate::MessageType::StreamResponseEnd,
        &start_header,
        correlation_id,
        0,
        schema_hash,
    );
    stream
        .write_all(&end_msg)
        .await
        .map_err(GossipError::Network)?;
    bytes_written_counter.fetch_add(end_msg.len(), Ordering::Relaxed);
    *bytes_since_flush += end_msg.len();

    stream.flush().await.map_err(GossipError::Network)?;
    *bytes_since_flush = 0;

    Ok(())
}

async fn process_read_result_io<S>(
    result: crate::handle::MessageReadResult,
    streaming_state: &mut crate::protocol::StreamingState,
    registry: &Arc<GossipRegistry>,
    peer_addr: SocketAddr,
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
            if let Some(expected) = registry.config.schema_hash {
                if schema_hash != Some(expected) {
                    warn!(
                        peer = %peer_addr,
                        expected = format_args!("{:016x}", expected),
                        received = schema_hash
                            .map(|hash| format!("{hash:016x}"))
                            .unwrap_or_else(|| "none".to_string()),
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
                    registry_weak: Arc::downgrade(registry),
                    peer_addr,
                    peer_id: None,
                    max_message_size: registry.config.max_message_size,
                    expected_schema_hash: registry.config.schema_hash,
                    aligned_pool: registry.connection_pool.aligned_bytes_pool(),
                    response_correlation: None,
                    response_writer: None,
                    tell_handler_sync: None,
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
            let write_start = perf.map(|_| Instant::now());
            direct_response_batch.push_bytes(correlation_id, payload.into_bytes());
            if let (Some(perf), Some(start)) = (perf, write_start) {
                perf.response_write_calls.fetch_add(1, Ordering::Relaxed);
                perf.response_write_ns
                    .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
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
        correlation_id: Option<u16>,
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
            let write_start = perf.map(|_| Instant::now());
            direct_response_batch.push_bytes(correlation_id, payload.into_bytes());
            *wrote_response_bytes = true;
            if let (Some(perf), Some(start)) = (perf, write_start) {
                perf.response_write_calls.fetch_add(1, Ordering::Relaxed);
                perf.response_write_ns
                    .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
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
            let has_legacy = ctx.sync_actor_handler.is_some() && (is_tell || is_ask);
            if !has_split && !has_legacy {
                return Ok(Some(crate::handle::MessageReadResult::Actor {
                    msg_type,
                    correlation_id,
                    actor_id,
                    type_hash,
                    schema_hash,
                    payload,
                }));
            }

            if let Some(expected) = ctx.expected_schema_hash
                && schema_hash != Some(expected)
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
            let write_start = perf.map(|_| Instant::now());
            direct_response_batch.push_bytes(correlation_id, payload.into_bytes());
            *wrote_response_bytes = true;
            if let (Some(perf), Some(start)) = (perf, write_start) {
                perf.response_write_calls.fetch_add(1, Ordering::Relaxed);
                perf.response_write_ns
                    .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
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
